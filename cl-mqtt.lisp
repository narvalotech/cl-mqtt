(ql:quickload "usocket")

(declaim (optimize (debug 3)))

(defmacro mqtt-with-broker ((host port socket stream) &body body)
  `(usocket:with-client-socket
       (,socket ,stream ,host ,port :element-type '(unsigned-byte 8))
     (progn ,@body)))

(defun read-bytes-recursively (stream response)
  (when (listen stream)
    (let ((byte (read-byte stream nil)))
      (when byte
        (vector-push-extend byte response)
        (read-bytes-recursively stream response)))))

(defun send-packet (socket stream data &key (wait-response nil))
  ;; Send a binary packet over a TCP stream/socket and receive its response
  (let ((bytes (make-array (length data)
                           :element-type '(unsigned-byte 8)
                           :initial-contents data))

        (response (make-array 1024
                              :adjustable t
                              :fill-pointer 0
                              :element-type '(unsigned-byte 8))))

    ;; Send the command
    (write-sequence bytes stream)
    (finish-output stream)

    (if wait-response
        ;; Read the response
        (multiple-value-bind (ready-sockets)
            (usocket:wait-for-input (list socket) :timeout 5)
          (if ready-sockets
              (progn (format t "reading..")
                     (read-bytes-recursively stream response)
                     (format t "rsp: ~A~%" response)
                     response)
              nil)))))

(defconstant +mqtt-opcodes+
  '(:connect 1
    :connect-ack 2
    :publish 3
    :publish-ack 4))

(defun mqtt-make-header-flags (opcode)
  (list (ash (getf +mqtt-opcodes+ opcode) 4)))

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
(format nil "~X" (encode-variable 268435456))
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
    (values result (subseq varint i))))

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
  (loop for char across input-string
        collect (char-code char)))

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
  (multiple-value-bind (len payload)
      (decode-variable (cdr packet))

    (if (not (equal len (length payload)))
        (error "invalid length"))

    payload))

(mqtt-make-packet :connect :client-id "lispy")
 ; => (16 18 0 4 77 81 84 84 5 2 0 5 0 0 5 108 105 115 112 121)
(extract-payload (mqtt-make-packet :connect :client-id "lispy"))
 ; => (0 4 77 81 84 84 5 2 0 5 0 0 5 108 105 115 112 121)

(defgeneric mqtt-decode-packet (opcode payload)
  (:documentation "De-serialize an MQTT packet into a MQTT packet object"))

(defmethod mqtt-decode-packet ((opcode (eql :connect-ack)) payload)
  ;; TODO use CLOS instead of crappy plists
  ;; TODO properly decode properties
  (let ((session-present (logbitp 0 (car payload)))
        (reason-code (nth 1 payload))
        (properties (subseq payload 2)))
    (list opcode
          :session-present session-present
          :reason-code reason-code
          :properties properties)))

(defmethod mqtt-decode-packet ((opcode (eql :publish)) payload)
  ;; TODO: make receiving QoS > 0 work
  ;; TODO: more ergonomic decoding
  (let* ((topic-length (decode-be-uint (subseq payload 0 2)))
         (topic (ascii->string (subseq payload 2 (+ 2 topic-length))))
         (packet-id nil)
         (prop-len (car (subseq payload (+ 2 topic-length))))
         (properties (if (zerop prop-len)
                         nil
                         (subseq payload (+ 2 1 topic-length) prop-len)))
         (payload (subseq payload
                          (+ 2 1 topic-length prop-len))))
    (list opcode
          :topic topic
          :packet-id packet-id
          :properties properties
          :payload payload)))

(mqtt-parse-packet
 '(48 20 0 13 104 101 108 108 111 47 109 121 116 111 112 105 99 0 1 2 3 4))
 ; => (:PUBLISH :TOPIC "hello/mytopic" :PACKET-ID NIL :PROPERTIES NIL :PAYLOAD
 ; (1 2 3 4))

(defun mqtt-parse-packet (packet)
  (if (equal (length packet) 0)
      (error "Empty packet"))
  (let* ((packet (coerce packet 'list))
         (opcode (decode-opcode packet))
         (payload (extract-payload packet)))
    (mqtt-decode-packet opcode payload)))

(mqtt-parse-packet '(32 9 0 0 6 34 0 10 33 0 20))
 ; => (:CONNECT-ACK :SESSION-PRESENT NIL :REASON-CODE 0 :PROPERTIES
 ; (6 34 0 10 33 0 20))

(mqtt-with-broker ("localhost" 1883 socket stream)
  (send-packet socket stream (mqtt-make-packet :connect :client-id "lispy")
               :wait-response t))
; reading..rsp: #(32 9 0 0 6 34 0 10 33 0 20)
;  => #(32 9 0 0 6 34 0 10 33 0 20)

(mqtt-with-broker ("localhost" 1883 socket stream)
  (mqtt-parse-packet
   (send-packet socket stream
                (mqtt-make-packet :connect :client-id "lispy")
                :wait-response t)))
; reading..rsp: #(32 9 0 0 6 34 0 10 33 0 20)
;  => (:CONNECT-ACK :SESSION-PRESENT NIL :REASON-CODE 0 :PROPERTIES
;  (6 34 0 10 33 0 20))

(mqtt-with-broker ("localhost" 1883 socket stream)
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
