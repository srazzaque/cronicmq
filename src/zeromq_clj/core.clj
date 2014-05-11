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
(def ^:private url-re #"^(tcp)://([^/]+)(/(.*))?$")

(defn- parse
  [url]
  (let [[_ protocol hostname topic] (re-matches url-re url)]
    {:url url
     :protocol protocol
     :hostname hostname
     :topic topic}))

(defn- send-on-socket
  [socket payload topic]
  (zmq/send-more! socket topic)
  (zmq/send! socket payload))

(defn publisher
  ([address]
     (publisher (implicit-context) address))
  ([context address]
     (let [socket (zmq/pub-socket! context address)
           info (parse address)]
       (if (nil? (:topic info))
         (fn [payload payload-topic]
           (send-on-socket socket payload payload-topic))
         (fn [payload]
           (send-on-socket socket payload (:topic info)))))))

(defn subscription
  [url]
  (let [info (parse url)
        sub-socket (zmq/sub-socket! (implicit-context) (str (:protocol info) "://" (:hostname info)))]
    (zmq/subscribe! sub-socket (:topic info))
    sub-socket))

(defn on-msg
  [socket & {do-f :do, while-f :while}]
  {:pre [socket do-f while-f]}
  nil)

(defn context
  [& args])

(defn close
  [& args])
