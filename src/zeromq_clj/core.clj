(ns zeromq-clj.core
  (:import (org.zeromq ZMQ)
           (com.lmax.disruptor EventHandler EventFactory)
           (com.lmax.disruptor.dsl Disruptor)
           (java.io ByteArrayOutputStream ObjectOutputStream ObjectInputStream ByteArrayInputStream)))

;; Credit: http://stackoverflow.com/questions/7701634/efficient-binary-serialization-for-clojure-java
(defn- serialize
  "Given a java.io.Serializable thing, serialize it to a byte array."
  [data]
  (let [buff (ByteArrayOutputStream. 1024)]
    (with-open [dos (ObjectOutputStream. buff)]
      (.writeObject dos data))
    (.toByteArray buff)))

;; Credit: http://stackoverflow.com/questions/7701634/efficient-binary-serialization-for-clojure-java
(defn- deserialize
  "Deserialize whatever is provided into a data structure"
  [bytes]
  (with-open [dis (ObjectInputStream. (ByteArrayInputStream. bytes))]
    (.readObject dis)))

(defn create-context
  []
  (ZMQ/context 1))

(defn create-sub-socket
  [context address topic]
  (io!
   (doto (.socket context ZMQ/SUB)
     (.connect address)
     (.setReceiveTimeOut 0) ;; Return straight away with nil if nothing
     (.subscribe (.getBytes topic)))))

(defn create-pub-socket
  [context address]
  (io!
   (let [socket (.socket context ZMQ/PUB)]
     (if (> (.bind socket address) 0)
       socket
       (throw (Exception. "Could not create publish socket."))))))

(defn publish
  [socket payload topic]
  (io!
   (.sendMore socket (.getBytes topic))
   (.send socket (serialize payload))))

(defn receive
  "Assumes two separate message parts - topic and then payload."
  [socket]
  (io!
   (let [topic (.recv socket)]
     (when topic
       (when (not (.hasReceiveMore socket))
         (throw (Exception. "No data received beyond topic header.")))
       (deserialize (.recv socket))))))

(defprotocol ^:private IMessageEnvelope
  (setMessage [this msg])
  (getMessage [this]))

(deftype ^:private MessageEnvelope [^:volatile-mutable data]
  IMessageEnvelope
  (setMessage [this msg]
    (set! data msg))
  (getMessage [this]
    data))

(defn- create-event-factory
  []
  (reify EventFactory
    (newInstance [this]
      (MessageEnvelope. nil))))

(defn- create-event-handler
  [func]
  (reify EventHandler
    (onEvent [this ev sequence eob?]
      (func (.getMessage ev)))))

(defn- create-message-puller-and-publisher
  "Pulls messages from the given socket, and publishes them into the given ring-buffer in the envelope."
  [socket ring-buffer]
  (fn []
    (while true
      (try
        (let [next-msg (receive socket)]
          (when next-msg
            (let [next-seq (.next ring-buffer)
                  next-event (.get ring-buffer next-seq)]
              (.setMessage next-event next-msg)
              (.publish ring-buffer next-seq))))
        (catch Exception e
          (println "Caught exception in message pulling loop:" e))))))

(defn on-msg
  "Spins up an lmax Disruptor to continuously pull messages off the provided socket and
  execute the given function on those messages."
  [socket func executor]
  (let [disruptor (doto (Disruptor. (create-event-factory) 65536 executor)
                    (.handleEventsWith (into-array EventHandler [(create-event-handler func)])))
        ring-buffer (.start disruptor)]
    (.execute executor (create-message-puller-and-publisher socket ring-buffer))
    disruptor))
