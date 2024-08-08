;; get'em debug frames
(declaim (optimize (debug 3)))

(defpackage :cl-mqtt
  (:use :common-lisp)
  (:nicknames :MQTT)
  (:local-nicknames (#:usocket #:usocket) (#:bt #:bordeaux-threads))
  (:export
   ;; functions
   #:connect-to-broker
   #:publish-with-response
   #:publish
   #:subscribe
   #:disconnect
   #:parse-packet
   #:parse-packets
   #:make-packet
   #:string->ascii
   #:ascii->string
   ;; macros
   #:with-broker
   ))

(in-package :cl-mqtt)

(defun last-element (vector)
  (elt vector (1- (length vector))))

;; (last-element #(0 1 2 3))
;;  ; => 3 (2 bits, #x3, #o3, #b11)

(defun packet-length-incomplete? (array)
  "Returns T when not enough bytes to decode packet length."
  (logbitp 7 (last-element array)))

(defun missing-bytes (array)
  "Returns the number of bytes array is missing to make a valid MQTT packet."
  (multiple-value-bind (packet-len offset)
      (decode-variable (coerce array 'list))
    (- (+ offset packet-len) (length array))))

(defun next-rx-size (array)
  "Returns the number of bytes necessary for a full packet."
  (cond
    ;; We need the opcode
    ((equal (length array) 0) 1)

    ;; We got the opcode, need (the first byte of the) packet size now
    ((equal (length array) 1) 1)

    ;; We got (parts) of the encoded packet length. We might need more.
    ((packet-length-incomplete? (subseq array 1)) 1)

    ;; We know the length, attempt to receive the payload.
    (t (missing-bytes (subseq array 1)))))

(defun stream-connected-p (stream)
  (and (not (equal (slot-value stream 'listen) :EOF))
       (not (equal (sb-sys:fd-stream-fd stream) -1))))

(defun read-bytes (stream packet num-bytes)
  "Phind made this (with some nudging)"
  (let ((temp-buffer (make-array num-bytes :element-type '(unsigned-byte 8))))
    (let ((read-count (read-sequence temp-buffer stream)))
      (dotimes (i read-count)
        (vector-push-extend (elt temp-buffer i) packet))
      packet)))

(defun read-from-socket (socket stream)
  (declare (ignore socket))             ; TODO: update API
  (let ((packet (make-array 1024
                            :adjustable t
                            :fill-pointer 0
                            :element-type '(unsigned-byte 8))))

    (loop while (and (not (zerop (next-rx-size packet)))
                     (stream-connected-p stream))
          do
             (read-bytes stream packet (next-rx-size packet)))
    packet))

(defun send-packet (socket stream data &key (wait-response nil))
  ;; Send a binary packet over a TCP stream/socket and receive its response
  (let ((bytes (make-array (length data)
                           :element-type '(unsigned-byte 8)
                           :initial-contents data)))

    ;; Send the command
    (write-sequence bytes stream)
    (finish-output stream)

    (if wait-response
        (read-from-socket socket stream))))

(defparameter +mqtt-opcodes+
  '(:connect 1
    :connect-ack 2
    :publish 3
    :publish-ack 4
    :subscribe 8
    :suback 9
    :pingreq 12
    :pingrsp 13
    :disconnect 14))

(defun mqtt-make-header-flags (opcode)
  (let ((id (getf +mqtt-opcodes+ opcode)))
    (list
     (case opcode
       (:subscribe (logior (ash id 4) #b0010))
       (t (ash id 4))))))

;; (mqtt-make-header-flags :connect)
;;  ; => (16)

(defun encode-variable (int)
  "Encodes an integer into the MQTT variable-intgth format"
  ;; See https://www.emqx.com/en/blog/introduction-to-mqtt-control-packets
  (when (> int (1- (ash 1 28)))
    (error "chonker"))

  (append
   (loop while (> int #x7f)
         collect
         (let ((new (logior #x80 (logand #x7f int))))
           (setf int (ash int -7))
           new))
   (list int)))

(defun decode-variable (varint)
  "Decodes a variable-length integer (sequence of bytes) into a lisp integer"
  (let ((i 0)
        (result 0))
    (loop for byte in varint
          do (progn
               (setf result (+ result (ash (logand byte #x7f) (* 7 i))))
               (incf i))
          until (not (logbitp 7 byte)))
    (values result i)))

(defun make-be-range (max)
  (loop for number downfrom (1- max) to 0 collect number))

(defun extract-byte (number index)
  (ldb (byte 8 (* index 8)) number))

(defun make-be-uint (octets number)
  (loop for pos in (make-be-range octets)
        collect (extract-byte number pos)))

(defun decode-be-uint (bytes)
  (loop for i from 0
        for byte in (reverse bytes)
        summing
        (ash byte (* i 8))))

(defgeneric make-packet (opcode &rest params)
  (:documentation "Encode an MQTT packet based on its opcode and a param-list"))

(defmethod make-packet :around (opcode &rest params)
  "Encodes the packet header and length. Returns a valid MQTT packet."
  (declare (ignore params))
  (let ((payload (call-next-method)))
    (append
     (mqtt-make-header-flags opcode)
     (encode-variable (length payload))
     payload)))

(defun make-field (payload)
  ;; TODO: check lengths
  (append (make-be-uint 2 (length payload))
          payload))

(defun string->ascii (input-string)
  (map 'list #'char-code input-string))

(defun ascii->string (ascii)
  (coerce
   (loop for char in ascii
         collect (code-char char)) 'string))

(defmethod make-packet ((opcode (eql :connect)) &rest params)
  ;; TODO: add setting username/password
  (let ((proto-name '(0 #x4 #x4d #x51 #x54 #x54))
        (proto-version '(5))
        (connect-flags '(#b00000010))
        ;; in seconds
        (keep-alive (make-be-uint 2 60))
        ;; no properties
        (properties '(0))
        ;; only client ID, no user/pass
        (payload (make-field (string->ascii (getf params :client-id)))))

    (append proto-name
            proto-version
            connect-flags
            keep-alive
            properties
            payload)))

(defmethod make-packet ((opcode (eql :publish)) &rest params)
  ;; TODO: utf-8 support
  ;; TODO: support strings in payload
  (let ((topic (string->ascii (getf params :topic)))
        ;; no packet id on QoS level 0
        (packet-id nil)
        ;; no support for properties yet
        (properties nil)
        (payload (getf params :payload)))

    (append (make-be-uint 2 (length topic))
            topic
            packet-id
            (make-be-uint 1 (length properties))
            properties
            payload)))

(defun topic->payload (topic)
  "Only intended for use with topics->subscribe-payload"
  (let ((payload (string->ascii topic)))
    (append (make-be-uint 2 (length payload))
            payload
            ;; Force max QoS to 0
            '(0))))

(defun topics->subscribe-payload (topics)
  (reduce #'append
          (map 'list #'topic->payload topics)))

;; (topics->subscribe-payload '("hello/world" "mytopic"))
 ; => (0 11 104 101 108 108 111 47 119 111 114 108 100 0 0 7 109 121 116 111 112 105
 ; 99 0)

(defmethod make-packet ((opcode (eql :subscribe)) &rest params)
  (let (;; app has to pass-in the packet identifier
        (packet-id (getf params :packet-id))
        ;; no support for properties yet
        (properties nil)
        (topics (getf params :topics)))

    (append (make-be-uint 2 packet-id)
            (make-be-uint 1 (length properties))
            properties
            (topics->subscribe-payload topics))))

(defmethod make-packet ((opcode (eql :pingreq)) &rest params)
  (declare (ignore params))
  '())

(defmethod make-packet ((opcode (eql :disconnect)) &rest params)
  (let ((reason-code (getf params :reason-code))
        ;; no support for properties yet
        (properties nil))

    (append (make-be-uint 1 reason-code)
            (make-be-uint 1 (length properties)))))

;; #x00 - normal disconnection
;; #x04 - disconnect with will
;; #x81 - malformed packet
;; #x82 - protocol error
;; #x8d - keep-alive timeout
;; #x8e - session taken over
;; #x93 - receive maximum exceeded
;; #x94 - topic alias invalid
;; #x95 - packet too large
;; #x98 - administrative action
;; #x9c - use another server
;; #x9d - server moved

(defun plist-key (plist value)
  (loop for (key val) on plist by #'cddr
        when (equal val value)
          return key))

(defun decode-opcode (packet)
  (plist-key +mqtt-opcodes+ (ash (first packet) -4)))

(defun extract-payload (packet)
  (multiple-value-bind (len offset)
      (decode-variable (cdr packet))
    (let ((payload (subseq (cdr packet) offset)))

      (if (> len (length payload))
          (error "invalid length"))

      payload)))

(defgeneric mqtt-decode-packet (opcode payload qos)
  (:documentation "De-serialize an MQTT packet into a MQTT packet object"))

(defmethod mqtt-decode-packet ((opcode t) payload qos)
  (list opcode
        :payload payload
        :qos qos))

(defmacro pull (buffer amount)
  "Pull `amount' elements out of the `buffer' list."
  `(let ((bytes (subseq ,buffer 0 ,amount)))
     (setf ,buffer (subseq ,buffer ,amount))
     bytes))

(defmethod mqtt-decode-packet ((opcode (eql :connect-ack)) payload qos)
  ;; TODO use CLOS instead of crappy plists
  ;; TODO properly decode properties
  (declare (ignore qos))
  (let* ((session-present (logbitp 0 (car (pull payload 1))))
         (reason-code (car (pull payload 1)))
         (properties payload))
    (list opcode
          :session-present session-present
          :reason-code reason-code
          :properties properties)))

(defmethod mqtt-decode-packet ((opcode (eql :publish)) payload qos)
  (let* ((topic-length (decode-be-uint (pull payload 2)))
         (topic (ascii->string (pull payload topic-length)))
         (packet-id (if (zerop qos)
                        nil
                        (pull payload 1)))
         (prop-len (car (pull payload 1)))
         (properties (if (zerop prop-len)
                         nil
                         (pull payload prop-len))))
    (list opcode
          :topic topic
          :packet-id packet-id
          :properties properties
          :payload payload)))

(defmethod mqtt-decode-packet ((opcode (eql :pingrsp)) payload qos)
  (list opcode))

(defmethod mqtt-decode-packet ((opcode (eql :disconnect)) payload qos)
  (let ((reason-code (decode-be-uint (pull payload 1))))
    (list opcode
          :reason-code reason-code)))

(defun decode-qos (opcode packet)
  "Returns QoS level of packet"
  ;; TODO: properly support retransmission
  (if (equal opcode :publish)
      (ash (logand #b0110 (first packet)) -1)
      ;; We don't support QoS for any other packet
      0))

(defun parse-packet (packet)
  (if (equal (length packet) 0)
      (return-from parse-packet nil))
  (let* ((packet (coerce packet 'list))
         (opcode (decode-opcode packet))
         (qos (decode-qos opcode packet))
         (payload (extract-payload packet)))
    (mqtt-decode-packet opcode payload qos)))

(defun mqtt-next-packet (packets)
  "Gives the offset with the start of the next MQTT packet"
  (multiple-value-bind (len offset)
      (decode-variable (cdr packets))
    (+ 1 len offset)))

(defun parse-packets (packets)
  "Parse a stream of packets into a list of MQTT packet objects"
  (let ((next (mqtt-next-packet packets)))
    (if (>= (length packets) next)
        (append
         (list (parse-packet (subseq packets 0 next)))
         (parse-packets (subseq packets next)))
        nil)))

(defmacro with-broker-socket ((host port socket stream) &body body)
  "Execute BODY with an open socket to a MQTT broker"
  `(usocket:with-client-socket
       (,socket ,stream ,host ,port :element-type '(unsigned-byte 8))
     (progn ,@body)))

;; (with-broker-socket ("localhost" 1883 socket stream)
;;   (send-packet socket stream (make-packet :connect :client-id "lispy")
;;                :wait-response t))
;;  ; => #(32 9 0 0 6 34 0 10 33 0 20)

;; (with-broker-socket ("localhost" 1883 socket stream)
;;   (parse-packet
;;    (send-packet socket stream
;;                 (make-packet :connect :client-id "lispy")
;;                 :wait-response t)))
;;  ; => (:CONNECT-ACK :SESSION-PRESENT NIL :REASON-CODE 0 :PROPERTIES
;;  ; (6 34 0 10 33 0 20))

(defun mqtt-connect (broker &key (client-id "cl-mqtt-client"))
  (let ((socket (getf broker :socket))
        (stream (getf broker :stream)))

    (parse-packet
     (send-packet socket stream
                  (make-packet :connect :client-id client-id)
                  :wait-response t))))

(defun make-broker (socket stream)
  "Makes an MQTT broker (client-side) instance."
  (list :socket socket :stream stream))

;; (with-broker-socket ("localhost" 1883 socket stream)
;;   (mqtt-connect (make-broker socket stream) :client-id "new-client"))
;;  ; => (:CONNECT-ACK :SESSION-PRESENT NIL :REASON-CODE 0 :PROPERTIES
;;  ; (6 34 0 10 33 0 20))

(defmacro with-broker ((host port broker &rest args) &body body)
  "Execute BODY with an open connection to a MQTT broker.
Specify the client name with the :client-id-str keyword param."
  (let* ((arg-pairs (loop for i from 0 below (length args) by 2
                          collect (cons (nth i args) (nth (1+ i) args))))
         (client-id-str (or (cdr (assoc :client-id-str arg-pairs)) "lispy")))
    `(usocket:with-client-socket
          (socket stream ,host ,port :element-type '(unsigned-byte 8))
        (let ((,broker (make-broker socket stream)))
          (mqtt-connect ,broker :client-id ,client-id-str)
          (progn ,@body)))))

;; (with-broker-socket ("localhost" 1883 socket stream)
;;   ;; Connect
;;   (parse-packet
;;    (send-packet socket stream
;;                 (make-packet :connect :client-id "lispy")
;;                 :wait-response t))

;;   ;; Send some dummy data
;;   ;; Since QoS is 0, we won't get a response
;;   (send-packet socket stream
;;                (make-packet :publish
;;                                  :topic "hello/mytopic"
;;                                  :payload (string->ascii "pretend-this-is-json"))))
;;  ; => NIL

;; (with-broker ("localhost" 1883 broker)
;;   ;; Send some dummy data
;;   ;; Since QoS is 0, we won't get a response
;;   (send-packet socket stream
;;                (make-packet :publish
;;                                  :topic "hello/mytopic"
;;                                  :payload (string->ascii "pretend-this-is-json"))))
;;  ; => NIL

(defun publish (broker topic payload)
  "Publish PAYLOAD to TOPIC with QoS 0 (no response)"
  (let ((socket (getf broker :socket))
        (stream (getf broker :stream))
        (encoded-payload))

    (setf encoded-payload
          (if (stringp payload)
              (string->ascii payload) ; encode if string
              payload))               ; else use the list of bytes given

    ;; Publish payload
    ;; Since QoS is 0, we won't get a response
    (send-packet socket stream
                 (make-packet :publish
                                   :topic topic
                                   :payload encoded-payload))))

;; (with-broker ("localhost" 1883 broker)
;;   (publish broker "hello/mytopic" "pretend-this-is-json"))
;;  ; => NIL

;; (with-broker ("localhost" 1883 broker)
;;   (let ((socket (getf broker :socket))
;;         (stream (getf broker :stream)))

;;     (send-packet socket stream
;;                  (make-packet :subscribe
;;                                    :packet-id 77
;;                                    :topics '("my/long/topic" "test-topic"))
;;                  :wait-response t)))
;;  ; => #(144 5 0 77 0 0 0)

(defun subscribe (broker topic)
  "Subscribe to TOPIC with QoS 0"
  (let ((socket (getf broker :socket))
        (stream (getf broker :stream)))

    ;; Subscribe to payload
    (send-packet socket stream
                 (make-packet :subscribe
                                   :packet-id 77
                                   :topics (list topic)))))

;; TODO:
;; - Try to match outstanding packets (maybe with queue?)

(defun broker-connected-p (broker)
  (stream-connected-p (getf broker :stream)))

(defun ping (broker)
  (let ((socket (getf broker :socket))
        (stream (getf broker :stream)))

    (send-packet socket stream (make-packet :pingreq))))

(defun disconnect (broker)
  (let ((socket (getf broker :socket))
        (stream (getf broker :stream)))

    (send-packet socket stream
                 (make-packet :disconnect :reason-code #x00))))

(defun publish-with-response (broker topic value resp-topic resp-filter)
  "Publish to a topic and wait for a response. This function will return the
payload of the packet for which `resp-filter' returns true"

  (let ((socket (getf broker :socket))
        (stream (getf broker :stream)))

    (subscribe broker resp-topic)
    (publish broker topic value)

    (loop while (broker-connected-p broker) do
      (let ((data (read-from-socket socket stream)))
        (when (> (length data) 0)
          (loop for packet in (parse-packets (coerce data 'list))
                do (when (funcall resp-filter packet)
                     (return-from publish-with-response
                       (getf (cdr packet) :payload)))))))))

(defun ping-thread-entrypoint (broker)
  (loop while (broker-connected-p broker) do
        (progn
          (ping broker)
          (sleep 5))))

(defun connect-to-broker (address port callback)
  "Open a connection to a broker, and call CALLBACK when data is received."
  (declare (type (function (t t) t) callback))

  (with-broker (address port broker)
    (let ((socket (getf broker :socket))
          (stream (getf broker :stream))
          (ping-thread))
      (format t "Connected. Entering receive loop.~%")

      (setf ping-thread (bt:make-thread
                         (lambda () (ping-thread-entrypoint broker))
                         :name "MQTT keepalive thread"))

      ;; subscribe to all topics
      (subscribe broker "#")

      (loop while (broker-connected-p broker) do
        (funcall callback broker (read-from-socket socket stream)))

      (bt:join-thread ping-thread))
    (format t "Exited receive loop.~%")))

;; Example usage
;;
;; (defun decode-and-print-packet (packet)
;;   ;; For now, we just parse it to stdout
;;   (if (> (length packet) 0)
;;       (let ((parsed (parse-packet packet)))
;;         (if (> (length packet) 0)
;;             (case (first parsed)
;;               (:pingrsp nil)
;;               (t (format t "Got packet: ~X~%" parsed)))))))

;; (defparameter *broker* nil)

;; (defun test-app-callback (broker data)
;;   (setf *broker* broker)
;;   (decode-and-print-packet data))

;; (connect-to-broker "localhost" 1883 #'test-app-callback)
;; (subscribe *broker* "#")
;; (publish *broker* "test/topic" "important data")
;; (disconnect *broker*)
