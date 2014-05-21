(ns zeromq-clj.serialization
  (:import (java.io ByteArrayOutputStream ObjectOutputStream ObjectInputStream ByteArrayInputStream)))

(defn serialize
  "Given a java.io.Serializable thing, serialize it to a byte array."
  [data]
  (let [buff (ByteArrayOutputStream.)]
    (with-open [dos (ObjectOutputStream. buff)]
      (.writeObject dos data))
    (.toByteArray buff)))

(defn deserialize
  "Deserialize whatever is provided into a data structure"
  [bytes]
  (with-open [dis (ObjectInputStream. (ByteArrayInputStream. bytes))]
    (.readObject dis)))
