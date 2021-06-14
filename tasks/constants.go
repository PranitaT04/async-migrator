package tasks

import "fmt"

const URL_PREFIX = "http://127.0.0.1:5002"
const LOCK_API = "/common/v1/lock/tenant/"
const RELEASE_API = "/common/v1/release/tenant/"
const GET_TENANTS_API = "/common/v1/tenants"

func isMcasEnabled(tenantId string) bool{
	fmt.Println(tenantId)
	return true
}

func serviceBlocked(tenantId string) bool{
	fmt.Println(tenantId)
	return true
}

