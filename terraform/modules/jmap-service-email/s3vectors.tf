# S3 Vectors bucket for search embeddings

resource "aws_s3vectors_vector_bucket" "search_vectors" {
  vector_bucket_name = "${local.name_prefix}-search-vectors"
}
