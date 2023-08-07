# Introduction

While [Delta Lake](https://delta.io/) is a well-known industry-standard approach to versioning structured datasets, sometimes Machine Learning / Deep Learning problems require _unstructured data_ for model training and testing. Examples include:

* Image / video files for computer vision problems
* Text files / documents for NLP problems
* Audio files for sound-related ML problems

Now, Delta Lake actually [supports image files as well](https://docs.databricks.com/en/machine-learning/reference-solutions/images-etl-inference.html), but there are limitations on file size, specifically:
> For large image files (average image size greater than 100 MB), Databricks recommends using the Delta table only to manage the metadata (list of file names) and loading the images from the object store using their paths when needed.

# Proposed Approach

Since Delta Lake already provides us great capabilities for structured data versioning, why don't we also use it for unstructured data?

The high-level flow:

1. We keep a catalog of the unstructured assets (in a given filepath), stored as a Delta Lake table
1. Whenever there are _new_ files, we update the catalog (which increments the Delta Table version number)
1. Whenever there are _changed_ files, we update the corresponding file's record in the catalog - perhaps with the checksum?
1. Whenever there are _removed_ files, we tombstone the correponding file's record in the catalog

There are some pretty obvious limitations to this approach - 

1. Does **not** facilitate time travel - we maintain just a record of the metadata, not the actual data files themselves, so we can't revert changed files, or recover removed files.
1. Needs a manual / frequently recurring job to scan the filepath and determine what has changed