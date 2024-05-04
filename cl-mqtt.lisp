(ql:quickload "usocket")

(defun read-bytes-recursively (stream response)
  (when (listen stream)
    (let ((byte (read-byte stream nil)))
      (when byte
        (vector-push-extend byte response)
        (read-bytes-recursively stream response)))))

(defun send-tcp-packet (host port data)
  (usocket:with-client-socket (socket stream host port :element-type '(unsigned-byte 8))
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

      ;; Read the response
      (multiple-value-bind (ready-sockets)
          (usocket:wait-for-input (list socket) :timeout 5)
        (if ready-sockets
            (progn (format t "reading..")
              (read-bytes-recursively stream response)
              (format t "rsp: ~X~%" response))
            "no-response"))
      )))

(defconstant +mqtt-opcodes+
  '(:connect 1
    :connect-ack 2))

(defun mqtt-make-header-flags (opcode)
  (ash (getf +mqtt-opcodes+ opcode) 4))

(mqtt-make-header-flags :connect)
 ; => 16 (5 bits, #x10, #o20, #b10000)

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

(defun mqtt-make-packet (opcode payload)
  (append
   (list (mqtt-make-header-flags opcode))
   (encode-variable (length payload))
   payload))

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

(defgeneric mqtt-make-packet (opcode &rest params)
  (:documentation "Encode an MQTT packet based on its opcode and a param-list"))

(defun make-field (payload)
  ;; TODO: check lengths
  (append (make-be-uint 2 (length payload))
          payload))

(defun string->ascii (input-string)
  (loop for char across input-string
        collect (char-code char)))

(string->ascii "hello")
 ; => (104 101 108 108 111)

(defmethod mqtt-make-packet :around (opcode &rest params)
  (declare (ignore params))
  (let ((payload (call-next-method)))
    (append
     (list (mqtt-make-header-flags opcode))
     (encode-variable (length payload))
     payload)))

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

(send-tcp-packet "localhost" 1883 (mqtt-make-packet :connect :client-id "lispy"))

