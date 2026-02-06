package email

// FindBodyPart recursively searches a BodyPart tree to find a part by ID.
// Returns nil if not found.
func FindBodyPart(root BodyPart, partID string) *BodyPart {
	if root.PartID == partID {
		return &root
	}
	for _, sub := range root.SubParts {
		if found := FindBodyPart(sub, partID); found != nil {
			return found
		}
	}
	return nil
}
