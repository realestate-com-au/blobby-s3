# Blobby::S3Store

This gem provides an S3-based implementation of the "store" interface defined by the "blobby" gem.  It's been packaged separately, to avoid adding dependencies to the core gem.

The simplest use-case is writing to a single bucket:

    s3_store = Blobby::S3Store.new("mybucket")
    s3_store["key"].write("something big")

Credentials can be provided, if required:

    credentials = { :access_key_id => "KEY, :secret_access_key => "SECRET" }
    s3_store = Blobby::S3Store.new("mybucket", credentials)

If none are specified, we'll look for them in [the normal places](https://blogs.aws.amazon.com/security/post/Tx3D6U6WSFGOK2H/A-New-and-Standardized-Way-to-Manage-Credentials-in-the-AWS-SDKs).
