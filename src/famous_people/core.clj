(ns famous-people.core
  (:gen-class
   :implements [com.amazonaws.services.lambda.runtime.RequestStreamHandler])
  (:require [amazonica.aws.s3 :as s3]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clj-time.format :as f]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [clj-time.periodic :as p]
            [clj-time.predicates :as pr]
            [dk.ative.docjure.spreadsheet :as doc]
            [environ.core :refer [env]]))

(def cred {:access-key (:access-key env)
           :secret-key (:secret-key env)
           :bucket-name (:bucket-name env)
           :endpoint   "us-east-1"})


(defmacro with-aws-credentials
  [credentials [aws-func & args]]
  `(let [updated-args# (if (and (:access-key ~credentials)
                                (:secret-key ~credentials))
                         (cons ~credentials (list ~@args))
                         (list ~@args))]
     (apply ~aws-func updated-args#)))


(defn extract-obj [obj-key]
  (:input-stream (with-aws-credentials cred
                   (s3/get-object
                    :bucket-name (:bucket-name cred)
                    :key obj-key))))


(defn extract-s3-obj []
  (let [obj-key "famous-people/famous_people.xlsx"]
    (->> obj-key
         extract-obj
         doc/load-workbook
         (doc/select-sheet "Sheet1")
         (doc/select-columns {:A :name})
         (remove (partial (comp nil? :name))))))


(defn rand-name [rows]
  (->> rows
       count
       rand-int
       (nth rows)
       :name
       (#(clojure.string/replace % #" " "_"))))


(defn consume-wiki-page [name]
  (-> (str "https://en.wikipedia.org/w/api.php?format=json&action=query"
           "&prop=extracts&exintro=&explaintext=&titles=" name)
      slurp
      (json/read-str :key-fn keyword)
      ((comp :extract second first :pages :query))
      (clojure.string/replace #"\([^)]*\)" "")))


(defn create-day-speech [text]
  {:uid (.toString (java.util.UUID/randomUUID))
   :updateDate (c/to-string (c/to-timestamp (t/now)))
   :titleText "Today's Famous Person"
   :mainText text})


(defn upload-file-s3 [json-data]
  (let [file-name "/tmp/flashbriefing.json"]
    (spit file-name json-data)
    (s3/put-object cred
                   :bucket-name (:bucket-name cred)
                   :key (:key cred)
                   :file (io/file file-name))))


(defn execute []
  (->> (extract-s3-obj)
       rand-name
       consume-wiki-page
       create-day-speech
       json/write-str
       upload-file-s3))


(defn -handleRequest
  [this input output context]
  (time (doall (execute))))
