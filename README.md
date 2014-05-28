# cronicmq

A Clojure messaging library built on top of ZeroMQ (using the JeroMQ implementation).

## Quickstart

In project.clj:

   (:dependencies [[io.cronic/cronicmq]])

An example publisher application:

   (require '[cronicmq.core :as cmq])
   (let [p (cmq/publisher "tcp://localhost:1234/theTopic")]
   	(dotimes [n 1000]
		 (p {:event-num n :message "Hello world!" :this-whole "map is serialized."}))
	(cmq/close :all))

An example subscriber application:

   (require '[cronicmq.core :as cmq])
   (let [s (cmq/subscriber "tcp://localhost:1234/theTopic")]
        (doseq [msg (take 10 (repeatedly s))]
	       (println "Message received:" msg))
   	(cmq/close :all))

More examples in the 'examples' folder.

## License

Copyright (C) 2014 Sandipan Razzaque (cronic.io)

Distributed under the Eclipse Public License either version 1.0 or (at your option) any later version.
