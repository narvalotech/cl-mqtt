(defsystem "cl-mqtt"
  :description "cl-mqtt: a WIP implementation of MQTT client"
  :version "0.0.1"
  :author "Jonathan Rico <jonathan@rico.live>"
  :licence "MIT"
  :depends-on ("usocket"
               "bordeaux-threads")
  :components ((:file "cl-mqtt"))
  :in-order-to ((test-op (test-op "cl-mqtt/tests"))))

(defsystem "cl-mqtt/tests"
  :depends-on ("cl-mqtt" "fiveam")
  :components ((:file "tests"))
  :perform (test-op (o s) (symbol-call :cl-mqtt/tests :run-all)))
