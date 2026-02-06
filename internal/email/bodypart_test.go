package email

import "testing"

func TestFindBodyPart_TopLevel(t *testing.T) {
	root := BodyPart{
		PartID: "1",
		Type:   "text/plain",
		BlobID: "blob-1",
	}

	part := FindBodyPart(root, "1")
	if part == nil {
		t.Fatal("expected to find part 1")
	}
	if part.BlobID != "blob-1" {
		t.Errorf("BlobID = %q, want %q", part.BlobID, "blob-1")
	}
}

func TestFindBodyPart_Nested(t *testing.T) {
	root := BodyPart{
		PartID: "0",
		Type:   "multipart/alternative",
		SubParts: []BodyPart{
			{PartID: "1", Type: "text/plain", BlobID: "blob-1", Charset: "utf-8"},
			{PartID: "2", Type: "text/html", BlobID: "blob-2", Charset: "utf-8"},
		},
	}

	part := FindBodyPart(root, "2")
	if part == nil {
		t.Fatal("expected to find part 2")
	}
	if part.BlobID != "blob-2" {
		t.Errorf("BlobID = %q, want %q", part.BlobID, "blob-2")
	}
	if part.Type != "text/html" {
		t.Errorf("Type = %q, want %q", part.Type, "text/html")
	}
}

func TestFindBodyPart_DeeplyNested(t *testing.T) {
	root := BodyPart{
		PartID: "0",
		Type:   "multipart/mixed",
		SubParts: []BodyPart{
			{
				PartID: "1",
				Type:   "multipart/alternative",
				SubParts: []BodyPart{
					{PartID: "1.1", Type: "text/plain", BlobID: "blob-1.1"},
					{PartID: "1.2", Type: "text/html", BlobID: "blob-1.2"},
				},
			},
			{PartID: "2", Type: "image/png", BlobID: "blob-2"},
		},
	}

	part := FindBodyPart(root, "1.2")
	if part == nil {
		t.Fatal("expected to find part 1.2")
	}
	if part.BlobID != "blob-1.2" {
		t.Errorf("BlobID = %q, want %q", part.BlobID, "blob-1.2")
	}
}

func TestFindBodyPart_NotFound(t *testing.T) {
	root := BodyPart{
		PartID: "1",
		Type:   "text/plain",
	}

	part := FindBodyPart(root, "nonexistent")
	if part != nil {
		t.Errorf("expected nil for nonexistent part, got %+v", part)
	}
}
