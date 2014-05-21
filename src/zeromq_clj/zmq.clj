(ns zeromq-clj.zmq
  "A thin wrapper around ZeroMQ. Don't use these functions directly - use what's exposed in core."
  (:import (org.zeromq ZMQ ZMQ$Context ZMQ$Socket)
           (java.io Closeable)))

(set! *warn-on-reflection* true)

(defn pub-socket!
  [^ZMQ$Context context ^String url]
  (doto (.socket context ZMQ/PUB)
    (.bind url)))

(defn sub-socket!
  [^ZMQ$Context context ^String url]
  (doto (.socket context ZMQ/SUB)
    (.connect url)))

(defn context!
  []
  (ZMQ/context 1))

(defn send-more!
  [^ZMQ$Socket socket ^bytes topic]
  (.sendMore socket topic))

(defn send!
  [^ZMQ$Socket socket ^bytes payload]
  (.send socket payload))

(defn subscribe!
  [^ZMQ$Socket socket ^bytes topic]
  (.subscribe socket topic))

(defn recv!
  [^ZMQ$Socket socket]
  (.recv socket))

(defn close!
  [^Closeable x]
  (.close x))

(defn has-more?
  [^ZMQ$Socket socket]
  (.hasReceiveMore socket))
