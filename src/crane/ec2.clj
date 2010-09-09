(ns 
    #^{:doc 
       "
samplecode
http://code.google.com/p/typica/wiki/TypicaSampleCode

javadoc
http://typica.s3.amazonaws.com/index.html)

You can not block on isRunning from an instance because the instance is not re-polling the server for status, the status that an in memory instance seems to have is based on the time the instance object was instantiated, not what is happenign right now.
"}
  crane.ec2
  (:use clojure.contrib.shell-out)
  (:use clj-ssh.ssh)
  (:use crane.config)
  (:import java.io.File)
  (:import java.util.ArrayList)
  (:import [com.xerox.amazonws.ec2 Jec2 
	    InstanceType LaunchConfiguration
	    ImageType EC2Exception
	    ReservationDescription
	    ReservationDescription$Instance
	    ReservedInstances]))

(defn ec2 [{key :key secretkey :secretkey}]
  (Jec2. key secretkey)) 

(defn describe-images [ec2]
  (let [params (ArrayList. )]
    (into []
	  (.describeImages ec2 params))))

(defn describe-instances [ec2]
  (let [params (ArrayList. )]
    (into []
	  (.describeInstances ec2 params))))

(defn attributes 
  [instance]
  {:public (.getDnsName instance)
   :private (.getPrivateDnsName instance)
   :zone (.getAvailabilityZone instance)})

(defn describe-security-groups [ec2]
  (let [params (ArrayList. )]
    (.describeSecurityGroups ec2 params)))

(defn describe-security-group [ec2 group]
  (filter #(= group (.getName %)) (describe-security-groups ec2)))

(defn create-security-group 
  [ec2 #^String name #^String desc]
  (.createSecurityGroup ec2 name desc))

(defn delete-security-group 
  [ec2 #^String name]
  (.deleteSecurityGroup ec2 name))

(defn auth-ports
"(ec2/auth-ports ec \"default\" \"tcp\" 22 22 \"0.0.0.0/0\")"
([ec2 group port] (auth-ports ec2 group "tcp" port port "0.0.0.0/0"))
([ec2 group protocol from-port to-port cidr-ip]
  (.authorizeSecurityGroupIngress ec2 group protocol 
				  from-port to-port cidr-ip)))

(defn auth-group-with-id [ec2 name security-group id]
  (.authorizeSecurityGroupIngress ec2 name security-group id))

(defn create-keypair
  "Creates a public/private keypair."
  [ec2 #^String name] 
  (.createKeyPair ec2 name))

(defn describe-keypairs [ec2]
  (.describeKeyPairs ec2 (ArrayList. )))

(defn pending? [x] 
  (.isPending x))

(defn running? [x]
  (.isRunning x))

(defn terminated? [x]
  (.isTerminated x))

(defn shutting-down? [x] 
  (.isShuttingDown x))

(defn instance-ids
"gets the instance ids from a seq of instances or reservations."
[xs]
  (let [inst (if (instance? ReservationDescription$Instance (first xs))
	       xs
	       (flatten (map 
			 #(.getInstances %)
			 xs)))]
    (map #(.getInstanceId %) inst)))

(defn terminate-instances 
  "usage: (terminate-instances ec (instance-ids (describe-instances ec)))"
  [ec2 instances]
  (.terminateInstances ec2 (into-array String instances)))

(defn kill-all [ec2]
  (terminate-instances
   ec2 (instance-ids (describe-instances ec2))))

(defn console-output [ec2 id]
  (.getConsoleOutput ec2 id))

(defn launch-config 
  "create a launch configuration for run-instances."
  [{:keys [image instance-type instances  
	   key-name group user-data monitoring zone]}]
  (let [groups (doto (ArrayList.) (.add group))]
    (doto (LaunchConfiguration. image instances instances)

      ;;setBlockDevicemappings(List<BlockDeviceMapping> blockDeviceMappings)
      ;;setConfigName(String configName)
      ;;setKernelId(String kernelId)
      ;;setMonitoring(boolean set)
      ;;setRamdiskId(String ramdiskId)
;;      (.setAvailabilityZone zone)   
      (.setInstanceType instance-type)
      (.setKeyName key-name)
      (.setSecurityGroup groups)
      (.setUserData user-data))))

(defn run-instance
  [ec2 image min max group userdata keyname type]
  (.runInstances ec2 image min max group userdata keyname type) (ArrayList.))

(defn run-instances 
  "provision n ec2 instances.  we assume that you want n instances and would like to reserve none if they are not available."
  [ec2 config]
  (.runInstances ec2 (launch-config config)))

(defn reservations-in-groups [res & groups]
  (let [in-groups (fn [belongs]
		    (some #(.contains groups %) belongs))]
    (filter #(in-groups (.getGroups %))  res)))

(defn find-reservations [ec2 cluster]
  (let [reservations (describe-instances ec2)]
    (reservations-in-groups 
     reservations 
     cluster cluster)))

(defn find-instances [pred reservations]
  (filter 
   pred  
   (flatten 
    (map 
     #(into [] (.getInstances %)) 
     reservations)))) 

(defn running-instances [ec2 cluster]
  (find-instances
   running?
   (find-reservations ec2 cluster)))

(defn already-running? 
  "takes ec2 and cluster a cluster name.

if you don't pass a number of instances, it is assumed that you mean the count of runnign istnaces is 1."
  ([ec2 cluster] (already-running? ec2 cluster 1))
  ([ec2 cluster n]
     (if (or (= n (count (running-instances ec2 cluster)))
             (> (count (running-instances ec2 cluster)) n))
       true
       false)))

(defn block-until-running 
  "
assums you are checking to see if a single node is up unless you pass the n number of nodes parameter.

it is returning the single first instance, maybe it should return all instances?
"
  ([ec2 cluster]
     (block-until-running ec2 cluster 1))
  ([ec2 cluster n]
     (do 
       (while (not (already-running? ec2 cluster n))
	 (println  "Provisioning... " 
		   (count (running-instances ec2 cluster))
		   " of " n 
		   " instances are up in " cluster)
	 (Thread/sleep 5000))
       (println "instances started...")
       ;;TODO: is there some kind of while-let so we don't need to find master a second time.
       (running-instances ec2 cluster))))

(defn ec2-instances 
"Launch cluster of k instances where k is specified by :instances in the conf map.  Cluster instances are in the same group as specified by :group in the conf map."
  [ec2 conf]
  (let [group-name (:group conf)
	cred (creds (:creds conf))
	defaults {:instances 1
		  :user-data (get-bytes "")
		  :monitoring false}
	new-conf (merge defaults conf cred)]
    (if (already-running? ec2 group-name (:instances new-conf))
      (running-instances ec2 group-name)
      (do 
	(run-instances ec2 new-conf)
	(block-until-running ec2 group-name (:instances new-conf))))))

(defn ec2-instance
  "launch a single instance"
  ([conf] (ec2-instance (ec2 (creds (:creds conf))) conf))
  ([ec2 conf]
     (first 
      (ec2-instances 
       ec2 
       (merge conf {:instances 1})))))

(defn in-ec2-session 
  "ssh session to ec2 instance."
  [instance conf f]
  (let [cred (creds (:creds conf))]
    (with-ssh-agent []
      (add-identity (:private-key-path cred))
      (let [inst (:public (attributes instance))
	    session (session inst :username (or (:user cred) "root")
			     :strict-host-key-checking :no)]
	    (with-connection session
	      (f session))))))