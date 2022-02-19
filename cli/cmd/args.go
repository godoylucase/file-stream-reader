package cmd

import "strings"

func argsMap(args []string) map[string]string {
	argMap := make(map[string]string, len(args))
	for _, arg := range args {
		split := strings.Split(arg, "=")
		key := split[:1]
		value := split[1:]
		argMap[key[0]] = value[0]
	}
	return argMap
}
