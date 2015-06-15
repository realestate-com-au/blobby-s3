# Blobby::S3Store

This gem provides an S3-based implementation of the "store" interface defined by the "rea-blob-storage" gem.  It's been packaged separately, to avoid adding dependencies to the core gem.

The simplest use-case is writing to a single bucket:

    s3_store = Blobby::S3Store.new("mybucket")
    s3_store["key"].write("something big")

Credentials can be provided, if required:

    credentials = { :access_key_id => "KEY, :secret_access_key => "SECRET" }
    s3_store = Blobby::S3Store.new("mybucket", credentials )

If none are specified, we'll look for `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
in the environment.

_Multiple_ buckets can be specified, in which case the S3Store will write _synchronously_ to the first,
and then spawn background threads to attempt to mirror to each of the remaining buckets.

    s3_store = Blobby::S3Store.new(["primary-bucket", "mirror1", "mirror2"])

### CI Plan
http://master.cd.vpc.realestate.com.au/browse/CP-BLOBS3
