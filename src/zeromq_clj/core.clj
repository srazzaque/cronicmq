(ns zeromq-clj.core
  (:import (org.zeromq ZMQ)
           (com.lmax.disruptor EventHandler EventFactory)
           (com.lmax.disruptor.dsl Disruptor)
           (java.io ByteArrayOutputStream ObjectOutputStream ObjectInputStream ByteArrayInputStream)))

(def ^:dynamic *polling-error-handler* (fn [e]
                                         (println "Caught exception in message pulling loop:" e)))

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
  "Creates a zmq 'SUB' socket, connects it to the given address, sets a timeout of 0 to make it
  non-blocking, and subscribes to the given topic."
  [context address topic]
  (io!
   (doto (.socket context ZMQ/SUB)
     (.connect address)
     (.setReceiveTimeOut 200) ;; Need to remain responsive to interrupts but not busy spin unnecessarily
     (.subscribe (.getBytes topic)))))

(defn create-pub-socket
  "Creates a zmq 'PUB' socket and binds to the given address."
  [context address]
  (io!
   (let [socket (.socket context ZMQ/PUB)]
     (if (> (.bind socket address) 0)
       socket
       (throw (Exception. "Could not create publish socket."))))))

(defn publish
  "Publishes an event on a given payload."
  [socket payload topic]
  (io!
   (.sendMore socket (.getBytes topic))
   (.send socket (serialize payload))))

(defn receive
  "Receives a message from a socket and deserializes it. Assumes two separate message parts - topic
  and then payload."
  [socket]
  (io!
   (let [topic (.recv socket)]
     (when topic
       (when (not (.hasReceiveMore socket))
         (throw (Exception. "No data received beyond topic header.")))
       (deserialize (.recv socket))))))

;; --------------------------------------------------------------------------------------------------------
;; Disruptor-related stuff

;; TODO: do we really need disruptor? Can we not just use core.async or quasar/pulsar channels? This
;; could really simplify both the code here and the cleanup code for clients.

(defprotocol ^:private IMessageEnvelope
  (setMessage [this msg])
  (getMessage [this]))

(deftype ^:private MessageEnvelope
  [^:volatile-mutable data]
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

(defn- not-interrupted
  []
  (not (.isInterrupted (Thread/currentThread))))

(defn- create-polling-function
  [socket ring-buffer] ;; TODO Maybe use a ZMQ poller here instead, can't see the benefits of doing so though.
  (fn []
    (while (not-interrupted)
      (try
        (let [next-msg (receive socket)]
          (when next-msg
            (let [next-seq (.next ring-buffer)
                  next-event (.get ring-buffer next-seq)]
              (.setMessage next-event next-msg)
              (.publish ring-buffer next-seq))))
        (catch Exception e
          (*polling-error-handler* e))))))

;; END Disruptor-related stuff
;; --------------------------------------------------------------------------------------------------------

(defn on-msg
  "Spins up an lmax Disruptor to continuously pull messages off the provided socket and
  execute the given function on those messages. Returns the Disruptor that was created as a result.

  To stop the listening processes, you need to:
  (.shutdown disruptor) ;; to kill the thread running the provided 'func' on incoming messages.
  (.shutdownNow ex) ;; to kill the thread that is polling the socket for messages.

  This is in addition to calling (.close) on any open contexts and sockets.

  To specify custom behaviour for exceptions thrown during polling handling, rebind
  *polling-error-handler* to a function accepting a single parameter - the thrown exception."
  [socket func executor]
  (let [disruptor (doto (Disruptor. (create-event-factory) 65536 executor)
                    (.handleEventsWith (into-array EventHandler [(create-event-handler func)])))
        ring-buffer (.start disruptor)]
    (.execute executor (create-polling-function socket ring-buffer))
    disruptor))
