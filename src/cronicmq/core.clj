(ns cronicmq.core
  "Main namespace for zeromq-clj."
  (:require [clojure.tools.logging :refer :all]
            [cronicmq.zmq :as zmq]
            [cronicmq.serialization :as s]))

(def ^:dynamic *context* (atom nil))

(defn- context
  []
  (or
   @*context*
   (reset! *context* (zmq/context!))))

(def ^:private url-re #"^(tcp)://([^/]+)(/(.*))?$")

(def ^:private open-sockets (atom {}))

(def ^:private next-id (atom 0))

(defn- get-next-id
  []
  (swap! next-id inc))

(defn- save-socket
  [sck]
  (let [m (meta sck)
        ctx (:context m)]
    (swap! open-sockets update-in [ctx] #(conj (or % #{}) sck)))
  sck)

(defn- remove-socket
  [sck]
  (let [m (meta sck)
        ctx (:context m)
        id (:id m)]
    (swap! open-sockets update-in [ctx] (fn
                                         [coll]
                                         (filter #(not (= id
                                                          (:id (meta %))))
                                                 coll)))))

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
           info (parse address)
           socket (zmq/pub-socket! context (str (:protocol info) "://" (:hostname info)))
           publisher-fn (if (nil? (:topic info))
                          (fn [^java.io.Serializable payload ^java.io.Serializable payload-topic]
                            (send-on-socket socket payload payload-topic))
                          (fn [^java.io.Serializable payload]
                            (send-on-socket socket payload (:topic info))))]
       (save-socket (with-meta publisher-fn {:socket socket
                                             :context context
                                             :id (get-next-id)})))))

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
        context (context)
        subscription-url (str (:protocol info) "://" (:hostname info))
        sub-socket (zmq/sub-socket! context subscription-url)
        subscription-fn (fn []
                          (receive-from-socket sub-socket))]
    (zmq/subscribe! sub-socket (s/serialize (:topic info)))
    (save-socket (with-meta subscription-fn {:socket sub-socket
                                             :context context
                                             :id (get-next-id)}))))

(defn- close-all
  []
  (let [ctx (context)]
    (doseq [s (@open-sockets ctx)]
      (zmq/close! (:socket (meta s)))
      (remove-socket s))
    (zmq/close! ctx)
    (swap! open-sockets dissoc ctx)
    (reset! *context* nil)))

(defn- close-single-socket
  [sock-fn]
  (let [sck (:socket (meta sock-fn))
        ctx (:context (meta sock-fn))]
    (when (not (or sck ctx))
      (throw (Exception. "Passed in socket fn did not have any metadata indicating which socket to close.")))
    (zmq/close! sck))
  (remove-socket sock-fn))

(defn close!
  "Closes the socket associated with a given socket function (created via 'publisher' or
   'subscription'). If passed an argument of :all, closes all open sockets associated with the
   global implicit context"
  [arg]
  (if (= arg :all)
    (close-all)
    (close-single-socket arg)))
