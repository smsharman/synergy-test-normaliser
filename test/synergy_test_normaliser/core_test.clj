(ns synergy-test-normaliser.core-test
  (:require [clojure.test :refer :all]
            [synergy-test-normaliser.core :refer :all]
            [clojure.java.io :as io]
            [cheshire.core :as json]
            ))

(comment
  (let [event (json/parse-stream (io/reader "test/resources/sns-input-message.json") true)]
    (println event)
    (println (json/generate-string event {:pretty true})))

  (let [event (json/parse-string (slurp "test/resources/sns-input-message.json") true)]
    (println event)
    (println (json/generate-string event {:pretty true})))

  (let [event (-> "test/resources/sns-input-message.json" slurp (json/parse-string true))]
    (println event)
    (println (json/generate-string event {:pretty true}))))