(defsystem "cl-mqtt"
  :description "cl-mqtt: a WIP implementation of MQTT client"
  :version "0.0.1"
  :author "Jonathan Rico <jonathan@rico.live>"
  :licence "MIT"
  :depends-on ("usocket"
               "bordeaux-threads")
  :components ((:file "cl-mqtt")))
