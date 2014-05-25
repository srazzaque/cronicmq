(ns zeromq-clj.core
  "Main namespace for zeromq-clj."
  (:require [clojure.tools.logging :refer :all]
            [zeromq-clj.zmq :as zmq]
            [zeromq-clj.serialization :as s]))

(def ^:dynamic *context* (atom nil))

(defn- context
  []
  (or
   @*context*
   (reset! *context* (zmq/context!))))

(def ^:private url-re #"^(tcp)://([^/]+)(/(.*))?$")

(def ^:private open-sockets (atom {}))

(defn- save-socket
  [sck]
  (swap! open-sockets update-in [(context)] #(conj (or % []) sck))
  sck)

(defn- parse
  [url]
  (let [[_ protocol hostname _ topic] (re-matches url-re url)]
    {:url url
     :protocol protocol
     :hostname hostname
     :topic topic}))

(defn- send-on-socket
  [socket ^java.io.Serializable payload ^java.io.Serializable topic]
  (zmq/send-more! socket (s/serialize topic))
  (zmq/send! socket (s/serialize payload)))

(defn publisher
  "Creates a publisher for the given url. Returns a function that can be used to publish a message.
   If the publisher is a multi-topic publisher, the function returned accepts two arguments - [payload topic].
   If the publisher is a single-topic publisher, the function return accepts one argument - [payload]."
  ([address]
     (let [context (context)
           socket (save-socket (zmq/pub-socket! context address))
           info (parse address)
           publisher-fn (if (nil? (:topic info))
                          (fn [^java.io.Serializable payload ^java.io.Serializable payload-topic]
                            (send-on-socket socket payload payload-topic))
                          (fn [^java.io.Serializable payload]
                            (send-on-socket socket payload (:topic info))))]
       (with-meta publisher-fn {:socket socket}))))

(defn- receive-from-socket
  [sub-socket]
  (let [topic (zmq/recv! sub-socket)]
    (if-not (and topic
                 (zmq/has-more? sub-socket))
      (throw (Exception. (str "No data received beyond topic header: " topic))))
    (s/deserialize (zmq/recv! sub-socket))))

(defn subscription
  "Opens a subscription to the given url. Returns a no-arg function that, when called, performs a blocking
   call to receive a message from the socket."
  [url]
  (let [info (parse url)
        subscription-url (str (:protocol info) "://" (:hostname info))
        sub-socket (save-socket (zmq/sub-socket! (context) subscription-url))
        subscription-fn (fn []
                          (receive-from-socket sub-socket))]
    (zmq/subscribe! sub-socket (s/serialize (:topic info)))
    (with-meta subscription-fn {:socket sub-socket})))

(defn- close-all
  []
  (let [ctx (context)]
    (doseq [s (@open-sockets ctx)]
      (zmq/close! s))
    (zmq/close! ctx)
    (swap! open-sockets dissoc ctx)
    (reset! *context* nil)))

(defn- close-single-socket
  [sock-fn]
  (let [sck (:socket (meta sock-fn))]
    (when (not sck)
      (throw (Exception. "Passed in socket fn did not have any metadata indicating which socket to close.")))
    (zmq/close! sck)))

(defn close!
  [arg]
  (if (= arg :all)
    (close-all)
    (close-single-socket arg)))
