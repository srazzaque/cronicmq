(ns zeromq-clj.core
  "The main namespace that user's should care about for the zeromq-clj library."
  (:require [clojure.tools.logging :refer :all]
            [zeromq-clj.zmq :as zmq]))

(def ^:private ctx (atom nil))

(defn- implicit-context
  []
  (or @ctx
      (reset! ctx (zmq/context!))))

;; TODO make it better and work for other protocols
(def ^:private url-re #"^(tcp)://([^/]+)/(.*)$")

(defn- get-topic
  [address]
  (let [[_ _ _ topic] (re-matches url-re address)]
    topic))

(defn- send-on-socket
  [socket payload topic]
  (zmq/send-more! socket topic)
  (zmq/send! socket payload))

(defn publisher
  ([address]
     (publisher (implicit-context) address))
  ([context address]
     (let [socket (zmq/pub-socket! context address)
           topic (get-topic address)]
       (if (empty? topic)
         (fn [payload payload-topic]
           (send-on-socket socket payload payload-topic))
         (fn [payload]
           (send-on-socket socket payload topic))))))

(defn subscription
  [])

(defn on-msg
  [& args]
  )

(defn context
  [& args])

(defn close
  [& args])
