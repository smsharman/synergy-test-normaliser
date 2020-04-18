(defproject synergy-test-normaliser "0.1.0-SNAPSHOT"
  :description "Synergy Example Event Handler"
  :url "http://synergyxm.ai/dispatcher"
  :license {:name "Hackthorn Innovation Ltd"
            :url ""}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [uswitch/lambada "0.1.2"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/tools.reader "1.3.2"]
                 [com.taoensso/timbre "4.10.0"]
                 [synergy-specs "0.1.8"]
                 [com.cognitect.aws/api "0.8.456"]
                 [com.cognitect.aws/endpoints "1.1.11.753"]
                 [com.cognitect.aws/sns "773.2.578.0"]
                 [com.cognitect.aws/ssm "794.2.640.0"]
                 [synergy-events-stdlib "0.1.3"]]
  :repl-options {:init-ns synergy-test-normaliser.core}
  :profiles {:uberjar {:aot :all}}
  :uberjar-name "synergy-test-normaliser.jar")
