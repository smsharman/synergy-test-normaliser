(ns synergy-test-normaliser.core
  (:require [uswitch.lambada.core :refer [deflambdafn]]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [cognitect.aws.client.api :as aws]
            [synergy-specs.events :as synspec]
            [clojure.spec.alpha :as s]
            [synergy-events-stdlib.core :as stdlib]
            [taoensso.timbre :as timbre
             :refer [log trace debug info warn error fatal report
                     logf tracef debugf infof warnf errorf fatalf reportf
                     spy get-env]])
  (:gen-class))

(def sns (aws/client {:api :sns}))

(def ssm (aws/client {:api :ssm}))

(def snsArnPrefix (atom ""))

(def eventStoreTopic (atom ""))

(def deliveryTopic "testInputTopic")

;; Event specific processing

(defn generate-new-event [jevent]
  (let [new-uuid (synspec/generate-new-eventId)]
  {
   :eventId new-uuid
   :parentId new-uuid
   :originId new-uuid
   :userId "1",
   :orgId "1",
   :eventVersion 1
   :eventAction (get jevent :eventAction)
   :eventData (get jevent :eventData)
   :eventTimestamp (synspec/generate-new-timestamp)
   }
  ))

(defn process-event [event-content event-type]
  (if (empty? @snsArnPrefix)
    (stdlib/set-up-topic-table snsArnPrefix eventStoreTopic ssm))
  (let [jevent (json/read-str event-content :key-fn keyword)
        tevent (generate-new-event jevent)
        wevent (synspec/wrap-std-event tevent)]
    (info "JEVENT : " jevent)
    (info "TEVENT : " tevent)
    (info "WEVENT : " wevent)
    (if (true? (get (stdlib/validate-message wevent) :status))
      (stdlib/send-to-topic deliveryTopic tevent @snsArnPrefix sns)
      (info "VALIDATION FAILED"))))

(defn handle-event [event]
  (info "Raw event: " (print-str event))
  (let [deduced-type (stdlib/check-event-type event)
        event-content (stdlib/get-event-data event deduced-type)]
  (process-event event-content deduced-type)))


(deflambdafn synergy-test-normaliser.core.Route
             [in out ctx]
             "Takes a JSON event in standard Synergy Event form from the Message field, convert to map and send to routing function"
             (let [event (json/read (io/reader in) :key-fn keyword)
                   res (handle-event event)]
               (with-open [w (io/writer out)]
                 (json/write res w))))
