(ns transducers.core
  (:require [clojure.core.async :refer [>! <! >!! <!! go chan buffer close! thread
                                        put! alts! alts!! timeout]]
            [clojure.string :refer [split]]))

(defn split-tokens [str]
  (split str #"\s"))

(defn decode [tokens]
  {:ip (get tokens 0)
   :method (get tokens 5)
   :url (get tokens 6)})

(defn extract-path [str]
  (first (split str #"\?")))

(def log-line-parser (comp (map split-tokens) (map decode)))

(into [] log-line-parser
      ["64.242.88.10 - - [07/Mar/2004:16:05:49 -0800] \"GET /twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables HTTP/1.1\" 401 12846"
       "64.242.88.10 - - [07/Mar/2004:16:06:51 -0800] \"GET /twiki/bin/rdiff/TWiki/NewUserTemplate?rev1=1.3&rev2=1.2 HTTP/1.1\" 200 4523"]
      )

(def path-extractor (comp (map :url)
                          (filter (complement nil?))
                          (map extract-path)))

(def method-extractor (keep :method))


(defn log-processor [file-path xf f]
  (let [lines (line-seq (clojure.java.io/reader file-path))]
    (transduce (comp log-line-parser xf) f lines)))

(defn counter
  "Incrementally counts the frequencies of elements"
  ([] {})
  ([m] m)
  ([m x] (assoc m x (inc (get m x 0)))))

(log-processor "data/access_log" path-extractor counter)
(log-processor "data/access_log" method-extractor counter)

(defn line-channel [file-path xf]
  (let [ch (chan 1 xf)]
    (go
      (loop [lines (line-seq (clojure.java.io/reader file-path))]
        (when-let [x (first lines)]
          (>! ch x)
          (recur (rest lines)))))
    ch))


(defn println-consumer [ch]
  (go
    (loop []
      (let [x (<! ch)]
        (println "Read: " x)
        (recur)))))


(defn channel-test []
  (println-consumer (line-channel "/Users/mdanila/Desktop/access_log"
                                  (comp log-line-parser (map :ip)))))


