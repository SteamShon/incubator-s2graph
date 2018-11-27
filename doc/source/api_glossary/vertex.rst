Manage Vertices (Optional)
===========================

Vertices are the two endpoints of an edge, and logically stored in columns of a service. If your use case requires storing metadata corresponding to vertices rather than edges, there are operations available on vertices as well.

Vertex Fields
***************
Unlike edges and their labels, properties of a vertex are not indexed nor require a predefined schema. The following fields are used when operating on vertices.

TODO: Field table

Basic Vertex Mutations
***********************

1. Insert - POST ``/mutate/vertex/insert/:serviceName/:columnName``
---------------------------------------------------------------------

Optional parameter serviceName, columnName can be used to remove them on the payload.

TODO: add example.

2. Delete - Post ``/mutate/vertex/delete/:serviceName/:columnName``
---------------------------------------------------------------------

TODO: add example.

3. DeleteAll - Post ``/mutate/vertex/delete/:serviceName/:columnName``
------------------------------------------------------------------------

TODO: add example.

Basic Vertex Query
************************

1. POST ``/graphs/getVertices``

TODO: add example.

ref: https://steamshon.gitbooks.io/s2graph-book/content/manage_vertices.html