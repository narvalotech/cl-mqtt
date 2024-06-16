(ql:quickload "usocket")
(ql:quickload "bordeaux-threads")

(declaim (optimize (debug 3)))

(defun read-bytes-recursively (stream response)
  (when (listen stream)
    (let ((byte (read-byte stream)))
      (when byte
        (vector-push-extend byte response)
        (read-bytes-recursively stream response)))))

(defun read-from-socket (socket stream)
  (let ((response (make-array 1024
                              :adjustable t
                              :fill-pointer 0
                              :element-type '(unsigned-byte 8))))
    (multiple-value-bind (ready-sockets)
        (usocket:wait-for-input (list socket) :timeout 5)
      (if ready-sockets
          (progn (read-bytes-recursively stream response)
                 response)
          nil))))

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

(defconstant +mqtt-opcodes+
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

(mqtt-make-header-flags :connect)
 ; => (16)

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

(format nil "~X" (encode-variable 128))
 ; => "(80 1)"
(format nil "~X" (encode-variable 16383))
 ; => "(FF 7F)"
(format nil "~X" (encode-variable 16384))
 ; => "(80 80 1)"
(format nil "~X" (encode-variable 2097151))
 ; => "(FF FF 7F)"
(format nil "~X" (encode-variable 2097152))
 ; => "(80 80 80 1)"
;; (format nil "~X" (encode-variable 268435456))
; -> should error out

(defun cont-bit (byte)
  (equal #x80 (logand #x80 byte)))

(cont-bit #x83)
 ; => T
(cont-bit #x7f)
 ; => NIL

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

(decode-variable '(#x80 #x01))
 ; => 128, NIL
(decode-variable '(#xFF #x7F))
 ; => 16383, NIL
(decode-variable '(#x80 #x80 #x1))
 ; => 16384, NIL
(decode-variable '(#xFF #xFF #x7F))
 ; => 2097151, NIL
(decode-variable '(#x80 #x80 #x80 #x1))
 ; => 2097152, NIL
(decode-variable '(#x80 #x80 #x80 #x1 #x2 #x3 #x4))
 ; => 2097152, (2 3 4)

(defun make-le-range (max)
  (loop for number from 0 to (- max 1) collect number))

(make-le-range 2)
 ; => (0 1)

(defun make-be-range (max)
  (loop for number downfrom (1- max) to 0 collect number))

(make-be-range 2)
 ; => (1 0)

(defun extract-byte (number index)
  (ldb (byte 8 (* index 8)) number))

(extract-byte #x7f88 1)
 ; => 127 (7 bits, #x7F, #o177, #b1111111)
(extract-byte #x7f88 0)
 ; => 136 (8 bits, #x88, #o210, #b10001000)

(defun make-uint (octets number)
  (loop for pos in (make-le-range octets)
        collect (extract-byte number pos)))

(defun make-be-uint (octets number)
  (loop for pos in (make-be-range octets)
        collect (extract-byte number pos)))

(defun decode-be-uint (bytes)
  (loop for i from 0
        for byte in (reverse bytes)
        summing
        (ash byte (* i 8))))

(decode-be-uint '(1 12))
 ; => 268 (9 bits, #x10C)
(decode-be-uint '(2 1 12))
 ; => 131340 (18 bits, #x2010C)

(defgeneric mqtt-make-packet (opcode &rest params)
  (:documentation "Encode an MQTT packet based on its opcode and a param-list"))

(defmethod mqtt-make-packet :around (opcode &rest params)
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

(string->ascii "hello")
 ; => (104 101 108 108 111)

(defun ascii->string (ascii)
  (coerce
   (loop for char in ascii
         collect (code-char char)) 'string))

(ascii->string '(104 101 108 108 111))
 ; => "hello"

(defmethod mqtt-make-packet ((opcode (eql :connect)) &rest params)
  ;; TODO: add setting username/password
  (let ((proto-name '(0 #x4 #x4d #x51 #x54 #x54))
        (proto-version '(5))
        (connect-flags '(#b00000010))
        ;; in seconds
        (keep-alive (make-be-uint 2 5))
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

(mqtt-make-packet :connect :client-id "lispy")
 ; => (16 18 0 4 77 81 84 84 5 2 0 5 0 0 5 108 105 115 112 121)

(defmethod mqtt-make-packet ((opcode (eql :publish)) &rest params)
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

(mqtt-make-packet :publish :topic "hello/mytopic" :payload '(1 2 3 4))
 ; => (48 20 0 13 104 101 108 108 111 47 109 121 116 111 112 105 99 0 1 2 3 4)

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

(topics->subscribe-payload '("hello/world" "mytopic"))
 ; => (0 11 104 101 108 108 111 47 119 111 114 108 100 0 0 7 109 121 116 111 112 105
 ; 99 0)

(defmethod mqtt-make-packet ((opcode (eql :subscribe)) &rest params)
  (let (;; app has to pass-in the packet identifier
        (packet-id (getf params :packet-id))
        ;; no support for properties yet
        (properties nil)
        (topics (getf params :topics)))

    (append (make-be-uint 2 packet-id)
            (make-be-uint 1 (length properties))
            properties
            (topics->subscribe-payload topics))))

(mqtt-make-packet :subscribe :packet-id 77 :topics '("my/long/topic" "test-topic"))
 ; => (130 32 0 77 0 0 13 109 121 47 108 111 110 103 47 116 111 112 105 99 0 0 10 116
 ; 101 115 116 45 116 111 112 105 99 0)

(defmethod mqtt-make-packet ((opcode (eql :pingreq)) &rest params)
  (declare (ignore params))
  '())

(mqtt-make-packet :pingreq)
 ; => (192 0)

(defmethod mqtt-make-packet ((opcode (eql :disconnect)) &rest params)
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
(mqtt-make-packet :disconnect :reason-code #x00)
 ; => (224 2 0 0)

(defun plist-key (plist value)
  (loop for (key val) on plist by #'cddr
        when (equal val value)
          return key))

(defun decode-opcode (packet)
  (plist-key +mqtt-opcodes+ (ash (first packet) -4)))

(decode-opcode (mqtt-make-header-flags :connect))
 ; => :CONNECT
(decode-opcode (mqtt-make-header-flags :connect-ack))
 ; => :CONNECT-ACK

(defun extract-payload (packet)
  (multiple-value-bind (len offset)
      (decode-variable (cdr packet))
    (let ((payload (subseq (cdr packet) offset)))

      (if (> len (length payload))
          (error "invalid length"))

      payload)))

(mqtt-make-packet :connect :client-id "lispy")
 ; => (16 18 0 4 77 81 84 84 5 2 0 5 0 0 5 108 105 115 112 121)
(extract-payload (mqtt-make-packet :connect :client-id "lispy"))
 ; => (0 4 77 81 84 84 5 2 0 5 0 0 5 108 105 115 112 121)

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

(mqtt-decode-packet :disconnect '(#x04 00) 0)
 ; => (:DISCONNECT :REASON-CODE 4)

(defun decode-qos (opcode packet)
  "Returns QoS level of packet"
  ;; TODO: properly support retransmission
  (if (equal opcode :publish)
      (ash (logand #b0110 (first packet)) -1)
      ;; We don't support QoS for any other packet
      0))

(defun mqtt-parse-packet (packet)
  (if (equal (length packet) 0)
      (return-from mqtt-parse-packet nil))
  (let* ((packet (coerce packet 'list))
         (opcode (decode-opcode packet))
         (qos (decode-qos opcode packet))
         (payload (extract-payload packet)))
    (mqtt-decode-packet opcode payload qos)))

(mqtt-parse-packet '(#xd0 0))
 ; => (NIL :PAYLOAD NIL :QOS 0)

(mqtt-parse-packet '(32 9 0 0 6 34 0 10 33 0 20))
 ; => (:CONNECT-ACK :SESSION-PRESENT NIL :REASON-CODE 0 :PROPERTIES
 ; (6 34 0 10 33 0 20))

(mqtt-parse-packet
 '(48 20 0 13 104 101 108 108 111 47 109 121 116 111 112 105 99 0 1 2 3 4))
 ; => (:PUBLISH :TOPIC "hello/mytopic" :PACKET-ID NIL :PROPERTIES NIL :PAYLOAD
 ; (1 2 3 4))

(defun mqtt-next-packet (packets)
  (multiple-value-bind (len offset)
      (decode-variable (cdr packets))
    (+ 1 len offset)))

(mqtt-next-packet '(32 9 0 0 6 34 0 10 33 0 20 32 9 0 0 6 34 0 10 33 0 20))
 ; => 11 (4 bits, #xB, #o13, #b1011)

(defun mqtt-parse-packets (packets)
  "Parse a stream of packets into a list of MQTT packet objects"
  (let ((next (mqtt-next-packet packets)))
    (if (>= (length packets) next)
        (append
         (list (mqtt-parse-packet (subseq packets 0 next)))
         (parse-packet (subseq packets next)))
        nil)))

(mqtt-parse-packets '(32 9 0 0 6 34 0 10 33 0 20))
 ; => ((:CONNECT-ACK :SESSION-PRESENT NIL :REASON-CODE 0 :PROPERTIES
 ;  (6 34 0 10 33 0 20)))
(mqtt-parse-packets '(32 9 0 0 6 34 0 10 33 0 20 48 20 0 13 104 101 108 108 111 47 109 121 116 111 112 105 99 0 1 2 3 4))
 ; => ((:CONNECT-ACK :SESSION-PRESENT NIL :REASON-CODE 0 :PROPERTIES
 ;  (6 34 0 10 33 0 20))
 ; (:PUBLISH :TOPIC "hello/mytopic" :PACKET-ID NIL :PROPERTIES NIL :PAYLOAD
 ;  (1 2 3 4)))

(defmacro mqtt-with-broker-socket ((host port socket stream) &body body)
  "Execute BODY with an open socket to a MQTT broker"
  `(usocket:with-client-socket
       (,socket ,stream ,host ,port :element-type '(unsigned-byte 8))
     (progn ,@body)))

(mqtt-with-broker-socket ("localhost" 1883 socket stream)
  (send-packet socket stream (mqtt-make-packet :connect :client-id "lispy")
               :wait-response t))
; reading..rsp: #(32 9 0 0 6 34 0 10 33 0 20)
;  => #(32 9 0 0 6 34 0 10 33 0 20)

(mqtt-with-broker-socket ("localhost" 1883 socket stream)
  (mqtt-parse-packet
   (send-packet socket stream
                (mqtt-make-packet :connect :client-id "lispy")
                :wait-response t)))
; reading..rsp: #(32 9 0 0 6 34 0 10 33 0 20)
;  => (:CONNECT-ACK :SESSION-PRESENT NIL :REASON-CODE 0 :PROPERTIES
;  (6 34 0 10 33 0 20))

(defun mqtt-connect (broker &key (client-id "cl-mqtt-client"))
  (let ((socket (getf broker :socket))
        (stream (getf broker :stream)))

    (mqtt-parse-packet
     (send-packet socket stream
                  (mqtt-make-packet :connect :client-id client-id)
                  :wait-response t))))

(defun make-broker (socket stream)
  "Makes an MQTT broker (client-side) instance."
  (list :socket socket :stream stream))

(mqtt-with-broker-socket ("localhost" 1883 socket stream)
  (mqtt-connect (make-broker socket stream) :client-id "new-client"))
; reading..rsp: #(32 9 0 0 6 34 0 10 33 0 20)
;  => (:CONNECT-ACK :SESSION-PRESENT NIL :REASON-CODE 0 :PROPERTIES
;  (6 34 0 10 33 0 20))

(defmacro mqtt-with-broker ((host port broker) &body body)
  "Execute BODY with an open connection to a MQTT broker"
  `(usocket:with-client-socket
       (socket stream ,host ,port :element-type '(unsigned-byte 8))
     (let ((,broker (make-broker socket stream)))
       (mqtt-connect ,broker :client-id "lispy")
       (progn ,@body))))

(mqtt-with-broker-socket ("localhost" 1883 socket stream)
  ;; Connect
  (mqtt-parse-packet
   (send-packet socket stream
                (mqtt-make-packet :connect :client-id "lispy")
                :wait-response t))

  ;; Send some dummy data
  ;; Since QoS is 0, we won't get a response
  (send-packet socket stream
               (mqtt-make-packet :publish
                                 :topic "hello/mytopic"
                                 :payload (string->ascii "pretend-this-is-json"))))
; reading..rsp: #(32 9 0 0 6 34 0 10 33 0 20)
;  => NIL

(mqtt-with-broker ("localhost" 1883 broker)
  ;; Send some dummy data
  ;; Since QoS is 0, we won't get a response
  (send-packet socket stream
               (mqtt-make-packet :publish
                                 :topic "hello/mytopic"
                                 :payload (string->ascii "pretend-this-is-json"))))
; reading..rsp: #(32 9 0 0 6 34 0 10 33 0 20)
;  => NIL

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
                 (mqtt-make-packet :publish
                                   :topic topic
                                   :payload encoded-payload))))

(mqtt-with-broker ("localhost" 1883 broker)
  (publish broker "hello/mytopic" "pretend-this-is-json"))
; reading..rsp: #(32 9 0 0 6 34 0 10 33 0 20)
;  => NIL

(defmacro with-fn-shadow ((orig new) &body body)
  `(let ((orig-backup))                 ; TODO: use gensym
     (if (fboundp ,orig)
         (progn
           (setf orig-backup (symbol-function ,orig))
           (setf (symbol-function ,orig) ,new)
           (unwind-protect (progn ,@body)
             (setf (symbol-function ,orig) orig-backup)))
         (error "Function ~A is not defined" ,orig))))

(defmacro with-var-shadow ((orig new) &body body)
  `(let ((orig-backup))                 ; TODO: use gensym
     (if (boundp ,orig)
         (progn
           (setf orig-backup (symbol-value ,orig))
           (setf (symbol-value ,orig) ,new)
           (unwind-protect (progn ,@body)
             (setf (symbol-value ,orig) orig-backup)))
         (error "Variable ~A is not defined" ,orig))))

(defun fake-publish (broker topic payload)
  (format t "Publishing:~% [broker] ~A~% [topic] ~A~% [payload] ~A~%"
          broker topic payload))

(with-fn-shadow ('publish #'fake-publish)
  (publish nil "hello/mytopic" "pretend-this-is-json"))
; Publishing:
;  [broker] NIL
;  [topic] hello/mytopic
;  [payload] pretend-this-is-json
;  => NIL

(mqtt-with-broker ("localhost" 1883 broker)
  (let ((socket (getf broker :socket))
        (stream (getf broker :stream)))

    (send-packet socket stream
                 (mqtt-make-packet :subscribe
                                   :packet-id 77
                                   :topics '("my/long/topic" "test-topic"))
                 :wait-response t)))
; reading..rsp: #(32 9 0 0 6 34 0 10 33 0 20)
; reading..rsp: #(144 5 0 77 0 0 0)
;  => #(144 5 0 77 0 0 0)

(defun subscribe (broker topic)
  "Subscribe to TOPIC with QoS 0"
  (let ((socket (getf broker :socket))
        (stream (getf broker :stream)))

    ;; Subscribe to payload
    (send-packet socket stream
                 (mqtt-make-packet :subscribe
                                   :packet-id 77
                                   :topics (list topic)))))

;; TODO:
;; - Try to match outstanding packets (maybe with queue?)

(defun mqtt-process-packet (packet)
  ;; For now, we just parse it to stdout
  (if (> (length packet) 0)
      (let ((parsed (mqtt-parse-packet packet)))
        (if (> (length packet) 0)
            (case (first parsed)
              (:pingrsp nil)
              (t (format t "Got packet: ~A~%" parsed)))))))

(defparameter *broker* nil)

(defun stream-connected-p (stream)
  (not (equal (slot-value stream 'listen) :EOF)))

(defun broker-connected-p (broker)
  (stream-connected-p (getf broker :stream)))

(defun ping (broker)
  (let ((socket (getf broker :socket))
        (stream (getf broker :stream)))

    (send-packet socket stream (mqtt-make-packet :pingreq))))

(defun disconnect (broker)
  (let ((socket (getf broker :socket))
        (stream (getf broker :stream)))

    (send-packet socket stream
                 (mqtt-make-packet :disconnect :reason-code #x00))))

(defun ping-thread-entrypoint (broker)
  (loop while (broker-connected-p broker) do
        (progn
          (ping broker)
          (sleep 5))))

(defun mqtt-connect-to-broker (address port callback)
  "Open a connection to a broker, and call CALLBACK when data is received."
  (declare (type (function (t t) t) callback))

  (mqtt-with-broker (address port broker)
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

(defun app-callback (broker data)
  (setf *broker* broker)
  (mqtt-process-packet data))

(mqtt-connect-to-broker "localhost" 1883 #'app-callback)

;; Run this from another thread
(subscribe *broker* "test/topic")
(publish *broker* "test/topic" "important data")
(disconnect *broker*)
