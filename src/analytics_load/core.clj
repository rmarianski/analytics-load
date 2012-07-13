(ns analytics-load.core
  (:require [clj-http.client :as client]
            [clojure.string :as string]
            [clojure.xml :as xml])
  (:import [java.util.concurrent Executors]))

;; (def host "localhost")
;; (def port "8080")

(def host "analytics.example.com")
(def port nil)

(def base-url (format "http://%s%s/geoserver/wfs"
                      host (if port (str ":" port) "")))

(def default-query-map {:Service "WFS"
                        :version "1.1.0"
                        :request "GetFeature"
                        ;; :typeName "topp:states"
                        :typeName "rob:pois"
                        :srsName "epsg:4326"})

(def possible-bboxes [["250773.92" "6500000.00" "290000.00" "6700000.00" "epsg:4326"]
                      ["280000.00" "6500000.00" "290000.00" "6700000.00" "epsg:4326"]
                      nil])

(def possible-maxfeatures [10 100 1000 nil])

(def possible-formats ["gml2" "json"])

(defn make-get-query-map [output-format max-features bbox]
  (merge
   {:outputFormat (or output-format "gml2")}
   (when max-features {:maxFeatures max-features})
   (when bbox {:bbox (string/join "," bbox)})))

(defn make-get [output-format max-features bbox]
  (let [query-params {:query-params (merge default-query-map
                                           (make-get-query-map
                                            output-format max-features bbox))}]
    (fn [] (client/get base-url query-params))))

(defn make-post [output-format max-features bbox]
  (let [xmldata (with-out-str
                  (xml/emit-element
                   {:tag "wfs:GetFeature"
                    :attrs (merge
                            {:service (:Service default-query-map)
                             :version (:version default-query-map "1.1.0")
                             :xmlns:wfs "http://www.opengis.net/wfs"
                             :xmlns:ogc "http://www.opengis.net/ogc"
                             :xmlns:xsi "http://www.w3.org/2001/XMLSchema-instance"
                             :xmlns:gml "http://www.opengis.net/gml"
                             :xsi:schemaLocation "http://www.opengis.net/wfs http://schemas.opengis.net/wfs/1.1.0/wfs.xsd"
                             }
                            (when max-features {:wfs:maxFeatures max-features}))
                    :content
                    [{:tag "wfs:Query" :attrs {:typeName (:typeName default-query-map)}
                      :content
                      (seq
                       (when bbox
                         [{:tag "ogc:Filter"
                           :content
                           [{:tag "ogc:BBOX"
                             :content
                             [{:tag "ogc:PropertyName" :content ["the_geom"]}
                              {:tag "gml:Envelope" :attrs {:srsName "http://www.opengis.net/gml/srs/epsg.xml#4326"}
                               :content
                               (let [[bb1 bb2 bb3 bb4] bbox]
                                 [{:tag "gml:lowerCorner" :content [(str bb1 " " bb2)]}
                                 {:tag "gml:upperCorner" :content [(str bb3 " " bb4)]}])}]}]}]
                         ))}]}))]
    (fn [] (client/post base-url {:body xmldata}))))

(defn make-request-fn
  [method output-format max-features bbox]
  (case method
    :get  (make-get  output-format max-features bbox)
    :post (make-post output-format max-features bbox)))

(def state (ref {}))

(defn init-state [& [total]]
  (dosync
   (ref-set state
            {:complete 0
             :success  0
             :failure  0
             :total    (or total 0)})))

(defn random-request-fn []
  (apply
   make-request-fn
   (map rand-nth [[:get :post] possible-formats possible-maxfeatures possible-bboxes])))

(defn update-state [is-successful]
  (dosync
   (alter state update-in [:complete] inc)
   (alter state update-in [(if is-successful :success :failure)] inc)))

(defn request-completed [result]
  (update-state (= 200 (:status result))))

(defn process-request [f]
  (request-completed (f)))

(defn go! [& [nthreads nloops]]
  (let [number (fn [x default] (Long. (or x default)))
        nthreads (number nthreads 1)
        nloops (number nloops 1)
        pool (Executors/newFixedThreadPool nthreads)
        total (* nloops nthreads)
        tasks (repeatedly
               total
               #(fn [] (process-request (random-request-fn))))]
    (init-state total)
    (doseq [future (.invokeAll pool tasks)]
      (.get future))
    (.shutdown pool)))
