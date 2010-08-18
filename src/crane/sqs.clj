(ns crane.sqs
  (:import
   (com.xerox.amazonws.sqs2 SQSUtils MessageQueue Message QueueService)))

;;for Typica usage examples see: http://typica.googlecode.com/svn/trunk/test/java/

(defn queue-service [{key :key secret-key :secretkey}]
  (QueueService. key secret-key true))

(defn get-queue [#^QueueService qs name]
  (.getMessageQueue qs name))

(defn get-or-create-queue [#^QueueService qs name]
  (.getOrCreateMessageQueue qs name))

(defn list-queues [#^QueueService qs]
 (.listMessageQueues qs nil))

(defn delete-queue [#^QueueService queue]
  (.deleteQueue queue))

(defn connect
  ([conf name]
     (connect (assoc conf :qname name) ))
  ([{key :key secret-key :secretkey qname :qname}]
     (SQSUtils/connectToQueue
      qname
      key secret-key)))

(defn recieve-msg
  ([queue]
     (.receiveMessage queue))
  ([queue n]
     (.receiveMessages queue count)))

(defn delete-message [queue m]
  (.deleteMessage queue m))

(defn send-msg [queue msg]
  (.sendMessage queue msg))

(defn message-body [m]
  (.getMessageBody m))

(defn count-messages [queue]
  (.getApproximateNumberOfMessages queue))