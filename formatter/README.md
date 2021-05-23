CREATE UNIQUE INDEX foo_size ON foo (size, smell, taste);

```
[ K<CREATE> K<UNIQUE> K<INDEX> N<foo_size> ]
[ K<ON> N<foo> ]
[ D()<
    []
    (
        [ N<size> ]
        [ N<smell> ]
        [ N<taste> ]
    )
    []
]
```

graph TB
    subgraph D
        3a["[]"]
        subgraph 3.1 ["()"]
            3.1a["N&lt;size&gt;"]
            3.1b["N&lt;smell&gt;"]
            3.1c["N&lt;taste&gt;"]
        end
        3b["[]"]
    end
    subgraph 2
        2a["K&lt;ON&gt;"]
        2b["N&lt;foo&gt;"]
    end
    subgraph 1
        1a["K&lt;CREATE&gt;"]
        1b["K&lt;UNIQUE&gt;"]
        1c["K&lt;INDEX&gt;"]
        1d["N&lt;foo_size&gt;"]
    end


CREATE INDEX title_idx_nulls_low ON films (title NULLS FIRST)

```
[ K<CREATE> K<INDEX> N<title_idx_nulls_low> ]
[ K<ON> N<films> ]
[ D()<
    []
    (
        [ N<title> K<NULLS> K<FIRST> ]
    )
    []
]
```

CREATE INDEX pointloc ON points USING GIST ( box( location, location ) )

```
[ K<CREATE> K<INDEX> N<pointloc> ]
[ K<ON> N<points> ]
[ K<USING> N<gist> ]
[ D()<
    []
    [ D()<
        [ N<box> ]
        (
            [ N<location> ]
            [ N<location> ]
        )
    ]
    []
]
```
