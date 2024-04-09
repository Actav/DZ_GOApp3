package utils

func UniqueTags(tagsList ...[]string) []string {
	var result []string
	uniqTags := make(map[string]struct{})

	for _, tags := range tagsList {
		for _, tag := range tags {
			if _, exists := uniqTags[tag]; !exists {
				uniqTags[tag] = struct{}{}
				result = append(result, tag)
			}
		}
	}

	return result
}
