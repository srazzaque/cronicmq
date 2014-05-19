(ns zeromq-clj.zmq
  "A thin wrapper around ZeroMQ. Don't use these functions directly - use what's exposed in core."
  (:import (org.zeromq ZMQ)))

(def pub-socket! nil)

(def sub-socket! nil)

(def context! nil)

(def send! nil)

(def subscribe! nil)

(def send-more! nil)

(def recv! nil)

(def close! nil)

(def has-more? nil)
