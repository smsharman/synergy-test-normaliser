(ns synergy-test-normaliser.core
  (:require [uswitch.lambada.core :refer [deflambdafn]]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [cognitect.aws.client.api :as aws]
            [synergy-specs.events :as synspec]
            [clojure.spec.alpha :as s]
            [taoensso.timbre :as timbre
             :refer [log trace debug info warn error fatal report
                     logf tracef debugf infof warnf errorf fatalf reportf
                     spy get-env]])
  (:gen-class))

(def sns (aws/client {:api :sns}))

(def ssm (aws/client {:api :ssm}))

(def routeTableParameters {
                           :arn-prefix "synergyDispatchTopicArnRoot"
                           :event-store-topic "synergyEventStoreTopic"
                           })

(def snsArnPrefix (atom ""))

(def eventStoreTopic (atom ""))

(defn getRouteTableParametersFromSSM []
  "Look up values in the SSM parameter store to be later used by the routing table"
  (let [snsPrefix (get-in (aws/invoke ssm {:op :GetParameter
                                           :request {:Name (get routeTableParameters :arn-prefix)}})
                          [:Parameter :Value])
        evStoreTopic (get-in (aws/invoke ssm {:op :GetParameter
                                              :request {:Name (get routeTableParameters :event-store-topic)}})
                             [:Parameter :Value])
        ]
    ;; //TODO: add error handling so if for any reason we can't get the values, this is noted
    {:snsPrefix snsPrefix :eventStoreTopic evStoreTopic}))

(defn setEventStoreTopic [parameter-map]
  "Set the eventStoreTopic atom with the required value"
  (swap! eventStoreTopic str (get parameter-map :eventStoreTopic)))

(defn setArnPrefix [parameter-map]
  "Set the snsArnPrefix atom with the required value"
  (swap! snsArnPrefix str (get parameter-map :snsPrefix)))

(def deliveryTopic "testInputTopic")

(defn gen-status-map
  "Generate a status map from the values provided"
  [status-code status-message return-value]
  (let [return-status-map {:status status-code :description status-message :return-value return-value}]
    return-status-map))

(defn set-up-route-table []
  (reset! snsArnPrefix "")
  (reset! eventStoreTopic "")
  (info "Routing table not found - setting up (probably first run for this Lambda instance")
  (let [route-paraneters (getRouteTableParametersFromSSM)]
    (setArnPrefix route-paraneters)
    (setEventStoreTopic route-paraneters)))

(defn send-to-topic
  ([thisTopic thisEvent]
   (send-to-topic thisTopic thisEvent ""))
  ([topic event note]
   (let [thisEventId (get event ::synspec/eventId)
         jsonEvent (json/write-str event)
         eventSNS (str @snsArnPrefix topic)
         snsSendResult (aws/invoke sns {:op :Publish :request {:TopicArn eventSNS
                                                               :Message  jsonEvent}})]
     (if (nil? (get snsSendResult :MessageId))
       (do
         (info "Error dispatching event to topic : " topic " (" note ") : " event)
         (gen-status-map false "error-dispatching-to-topic" {:eventId thisEventId
                                                             :error snsSendResult}))
       (do
         (info "Dispatching event to topic : " eventSNS " (" note ") : " event)
         (gen-status-map true "dispatched-to-topic" {:eventId   thisEventId
                                                     :messageId (get snsSendResult :MessageId)}))))))

(defn validate-message [inbound-message]
  (if (s/valid? ::synspec/synergyEvent inbound-message)
    (gen-status-map true "valid-inbound-message" {})
    (gen-status-map false "invalid-inbound-message" (s/explain-data ::synspec/synergyEvent inbound-message))))

(defn check-event-type [event]
  "Deduce what type of AWS event triggered the Lambda, based on message field content"
  (let [checkRecordType (get event :Records)
        evCheck1 (get (first (get event :Records)) :eventSource)
        evCheck2 (get (first (get event :Records)) :EventSource)
        srcCheck (get event :source)
        routeKeyCheck (get event :routeKey)
        ]
    (cond (and (not (nil? checkRecordType)) (= evCheck1 "aws:sqs"))
          "SQS"
          (and (not (nil? checkRecordType)) (= evCheck1 "aws:s3"))
          "S3"
          (and (not (nil? checkRecordType)) (= evCheck2 "aws:sns"))
          "SNS"
          (and (nil? checkRecordType) (= srcCheck "aws.events"))
          "Cloudwatch"
          (and (nil? checkRecordType) (not (nil? routeKeyCheck)))
          "APIGateway"
          :else
          "NOTKNOWN")))

(defn get-event-data [event src-type]
  "Extract the data from an inbound message based on the event type"
  (cond (= src-type "SNS")
        (get (get (first (get event :Records)) :Sns) :Message)
        (= src-type "SQS")
        (get (first (get event :Records)) :body)
        (= src-type "S3")
        (first (get event :Records))
        (= src-type "Cloudwatch")
        event
        (= src-type "APIGateway")
        {:routeKey (get event :routeKey)
         :body (get event :body)}
        :else
        nil))

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
    (set-up-route-table))
  (let [jevent (json/read-str event-content :key-fn keyword)
        tevent (generate-new-event jevent)
        wevent (synspec/wrap-std-event tevent)]
    (info "JEVENT : " jevent)
    (info "TEVENT : " tevent)
    (info "WEVENT : " wevent)
    (if (true? (get (validate-message wevent) :status))
      (send-to-topic deliveryTopic tevent)
      (info "VALIDATION FAILED"))))

(defn handle-event [event]
  (info "Raw event: " (print-str event))
  (let [deduced-type (check-event-type event)
        event-content (get-event-data event deduced-type)]
  (process-event event-content deduced-type)))


(deflambdafn synergy-test-normaliser.core.Route
             [in out ctx]
             "Takes a JSON event in standard Synergy Event form from the Message field, convert to map and send to routing function"
             (let [event (json/read (io/reader in) :key-fn keyword)
                   res (handle-event event)]
               (with-open [w (io/writer out)]
                 (json/write res w))))
