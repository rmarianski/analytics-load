# analytics-load

Generate random wfs requests to a geoserver instance.

## Usage

Fire up a swank server, compile, and:

```clj
(in-ns 'analytics-load.core)
(let [n-threads 10
      n-loops   100]
  (future (go! n-threads n-loops)))
```

Then you can keep track of progress with:
```clj
@state
```

## License

Copyright © 2012 Robert Marianski

Distributed under the Eclipse Public License, the same as Clojure.
