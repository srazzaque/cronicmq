(ns zeromq-clj.core
  "Main namespace for zeromq-clj."
  (:require [clojure.tools.logging :refer :all]
            [zeromq-clj.zmq :as zmq]))

(def ^:dynamic *context* (delay (zmq/context!)))

(def ^:private url-re #"^(tcp)://([^/]+)(/(.*))?$")

(def ^:private open-sockets (atom {}))

(defn- save-socket
  [sck]
  (swap! open-sockets update-in [@*context*] #(conj (or % []) sck))
  sck)

(defn- parse
  [url]
  (let [[_ protocol hostname _ topic] (re-matches url-re url)]
    {:url url
     :protocol protocol
     :hostname hostname
     :topic topic}))

(defn- send-on-socket
  [socket payload topic]
  (zmq/send-more! socket topic)
  (zmq/send! socket payload))

(defn publisher
  "Creates a publisher for the given url. Returns a function that can be used to publish a message.
   If the publisher is a multi-topic publisher, the function returned accepts two arguments - [payload topic].
   If the publisher is a single-topic publisher, the function return accepts one argument - [payload]."
  ([address]
     (let [context @*context*
           socket (save-socket (zmq/pub-socket! context address))
           info (parse address)]
       (if (nil? (:topic info))
         (fn [payload payload-topic]
           (send-on-socket socket payload payload-topic))
         (fn [payload]
           (send-on-socket socket payload (:topic info)))))))

(defn- receive-from-socket
  [sub-socket]
  (let [topic (save-socket (zmq/recv! sub-socket))]
    (if-not (and topic
                 (zmq/has-more? sub-socket))
      (throw (Exception. "No data received beyond topic header")))
    (zmq/recv! sub-socket)))

(defn subscription
  "Opens a subscription to the given url. Returns a no-arg function that, when called, performs a blocking
   call to receive a message from the socket."
  [url]
  (let [info (parse url)
        subscription-url (str (:protocol info) "://" (:hostname info))
        sub-socket (save-socket (zmq/sub-socket! @*context* subscription-url))]
    (zmq/subscribe! sub-socket (:topic info))
    (fn []
      (receive-from-socket sub-socket))))

(defn close!
  [& args]
  (doseq [s (@open-sockets @*context*)]
    (zmq/close! s))
  (zmq/close! @*context*)
  (swap! open-sockets dissoc @*context*))
