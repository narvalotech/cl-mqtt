(ql:quickload :cl-json)

(json:decode-json-from-string "{
  \"state\": \"ON\",
  \"brightness\": 215,
  \"color_temp\": 325
}")
 ; => ((:STATE . "ON") (:BRIGHTNESS . 215) (:COLOR--TEMP . 325))

(json:decode-json-from-string "{
  \"state\": \"ON\",
    \"params\": {
    \"brightness\": 215,
    \"color_temp\": 325
    },
  \"other\": \"stuff\"
}")
 ; => ((:STATE . "ON") (:PARAMS (:BRIGHTNESS . 215) (:COLOR--TEMP . 325))
 ; (:OTHER . "stuff"))
(defparameter *switch-sample-payload*
"{
    \"battery\": 74,
    \"action\": \"brightness_move_up\",
    \"linkquality\": 76
}")
(json:decode-json-from-string *switch-sample-payload*)
 ; => ((:BATTERY . 74) (:ACTION . "brightness_move_up") (:LINKQUALITY . 76))

(defun jv (json-string key)
  "Returns the value pointed to by `KEY' in `JSON-STRING'. Use like `GETF'."
  (let ((decoded (json:decode-json-from-string json-string)))
    (loop for el in decoded do
          (if (equal (car el) key) (return (cdr el))))))

(jv *switch-sample-payload* :action)
 ; => "brightness_move_up"
(jv *switch-sample-payload* :linkquality)
 ; => 76 (7 bits, #x4C, #o114, #b1001100)
