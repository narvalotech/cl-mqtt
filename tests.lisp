(defpackage #:cl-mqtt/tests
  (:use :cl :fiveam)
  (:import-from #:cl-mqtt
                :encode-variable
                :decode-variable
                :packet-length-incomplete?
                :next-rx-size

                :extract-byte
                :make-be-range
                :make-be-uint
                :decode-be-uint

                :string->ascii
                :ascii->string

                :make-packet
                :mqtt-make-header-flags
                :decode-opcode
                :extract-payload
                :mqtt-decode-packet
                :parse-packet

                :mqtt-next-packet
                :parse-packets
                )
  (:export #:run-all))

(in-package :cl-mqtt/tests)

(defun run-all ()
  (run!))

(test encode-variable
  (is (equal '(#x80 #x1) (encode-variable 128)))
  (is (equal '(#xFF #x7F) (encode-variable 16383)))
  (is (equal '(#x80 #x80 #x1) (encode-variable 16384)))
  (is (equal '(#xFF #xFF #x7F) (encode-variable 2097151)))
  (is (equal '(#x80 #x80 #x80 #x1) (encode-variable 2097152)))
  (signals error (encode-variable 268435456)))

(test decode-variable
  (equal '(128 2) (multiple-value-list
                   (decode-variable '(#x80 #x01))))

  (equal '(16383 2) (multiple-value-list
                     (decode-variable '(#xFF #x7F))))

  (equal '(2097151 3) (multiple-value-list
                       (decode-variable '(#xFF #xFF #x7F))))

  (equal '(2097152 4) (multiple-value-list
                       (decode-variable '(#x80 #x80 #x80 #x1))))

  (equal '(2097152 4) (multiple-value-list
                       (decode-variable '(#x80 #x80 #x80 #x1 #x2 #x3 #x4)))))

(test packet-length-incomplete?
  (is (equal t (packet-length-incomplete? #(#x80))))
  (is (equal nil (packet-length-incomplete? #(#x80 #x80 #x01)))))

(test next-rx-size
  (is (= 1 (next-rx-size #(144))))
  (is (= 5 (next-rx-size #(144 5))))
  (is (= 1 (next-rx-size #(144 5 0 77 0 0))))
  (is (= 0 (next-rx-size #(144 5 0 77 0 0 0)))))

(test number-coding
  (is (equal '(1 0) (make-be-range 2)))
  (is (= #x7f (extract-byte #x7f88 1)))
  (is (= #x88 (extract-byte #x7f88 0)))
  (is (= #x10c (decode-be-uint '(1 12))))
  (is (= #x2010c (decode-be-uint '(2 1 12)))))

(test ascii
  (is (equal '(104 101 108 108 111) (string->ascii "hello")))
  (is (equal "hello"(ascii->string '(104 101 108 108 111)))))

(test make-packet
  (is (equal
       '(16 18 0 4 77 81 84 84 5 2 0 60 0 0 5 108 105 115 112 121)
      (make-packet :connect :client-id "lispy")))

  (is (equal
       '(48 20 0 13 104 101 108 108 111 47 109 121 116 111 112 105 99 0 1 2 3 4)
       (make-packet :publish :topic "hello/mytopic" :payload '(1 2 3 4))))

  (is (equal
       '(130 32 0 77 0 0 13 109 121 47 108 111 110 103 47 116 111 112 105 99 0 0 10 116 101 115 116 45 116 111 112 105 99 0)
       (make-packet :subscribe :packet-id 77 :topics '("my/long/topic" "test-topic"))))

  (is (equal
       '(192 0)
       (make-packet :pingreq)))

  (is (equal
       '(224 2 0 0)
       (make-packet :disconnect :reason-code #x00)))
  )

(test decode-packets
  (is (equal '(0 4 77 81 84 84 5 2 0 60 0 0 5 108 105 115 112 121)
             (extract-payload (make-packet :connect :client-id "lispy"))))
  (is (equal :connect (decode-opcode (mqtt-make-header-flags :connect))))
  (is (equal :connect-ack (decode-opcode (mqtt-make-header-flags :connect-ack))))
  (is (equal '(:DISCONNECT :REASON-CODE 4)
             (mqtt-decode-packet :disconnect '(#x04 00) 0))))

(test parse-packet
  (is (equal '(:PINGRSP)
             (parse-packet '(#xd0 0))))

  (is (equal '(:CONNECT-ACK :SESSION-PRESENT NIL :REASON-CODE 0 :PROPERTIES (6 34 0 10 33 0 20))
             (parse-packet '(32 9 0 0 6 34 0 10 33 0 20))))

  (is (equal '(:PUBLISH :TOPIC "hello/mytopic" :PACKET-ID NIL :PROPERTIES NIL :PAYLOAD (1 2 3 4))
             (parse-packet '(48 20 0 13 104 101 108 108 111 47 109 121 116 111 112 105 99 0 1 2 3 4)))))

(test mqtt-next-packet
  (is (= 11 (mqtt-next-packet '(32 9 0 0 6 34 0 10 33 0 20 32 9 0 0 6 34 0 10 33 0 20)))))

(test parse-packets
  (is (equal '((:CONNECT-ACK :SESSION-PRESENT NIL :REASON-CODE 0 :PROPERTIES (6 34 0 10 33 0 20)))
             (parse-packets '(32 9 0 0 6 34 0 10 33 0 20))))
  (is (equal '((:CONNECT-ACK :SESSION-PRESENT NIL :REASON-CODE 0 :PROPERTIES (6 34 0 10 33 0 20))
               (:PUBLISH :TOPIC "hello/mytopic" :PACKET-ID NIL :PROPERTIES NIL :PAYLOAD (1 2 3 4)))
             (parse-packets '(32 9 0 0 6 34 0 10 33 0 20 48 20 0 13 104 101 108 108 111 47 109 121 116 111 112 105 99 0 1 2 3 4))))
  (is (equal '((:CONNECT-ACK :SESSION-PRESENT NIL :REASON-CODE 0 :PROPERTIES (6 34 0 10 33 0 20))
               (:PUBLISH :TOPIC "hello/mytopic" :PACKET-ID NIL :PROPERTIES NIL :PAYLOAD (1 2 3 4))
               (:SUBACK :PAYLOAD (0 77 0 0 0) :QOS 0))
             (parse-packets '(32 9 0 0 6 34 0 10 33 0 20 48 20 0 13 104 101 108 108 111 47 109 121 116 111 112 105 99 0 1 2 3 4 144 5 0 77 0 0 0)))))
