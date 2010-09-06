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

(defn delete-message [queue message-or-handle]
  (.deleteMessage queue message-or-handle))

(defn send-msg [queue msg]
  (.sendMessage queue msg))

(defn send-pkg
  "creates n paritions of a seq s and sends the parititons as messages."
  [q s n]
  (let [p (/ (count s) n)]
  (doall (map #(send-msg q (pr-str %))
	      (partition p p [] s)))))

(defn recieve-msg
  ([queue]
     (.receiveMessage queue))
  ([queue n]
     (.receiveMessages queue count)))

;;TODO: this with-msg is a little funky...it exists so consuders of the processed body can then delete the message from the queue.
;;work it out based on use cases from fetcher and crawler.
(defn with-msg [f]
  (fn [msg]
    (let [body (.getMessageBody msg)
	  id (.getMessageId msg)
	  handle (.getReceiptHandle msg)]
      [[id handle body] (f body)])))

(defn consume-msg [q]
  (let [msg (recieve-msg q)
	body (.getMessageBody msg)
	handle (.getReceiptHandle msg)
	_  (delete-message q handle)]
    body))

(def consume-pkg (comp read-string consume-msg)) 

(defn message-body [m]
  (.getMessageBody m))

(defn count-messages [queue]
  (.getApproximateNumberOfMessages queue))