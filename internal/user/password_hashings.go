package user

// DefaultPasswordHashingAlgorithm is the default password hashing scheme for user profiles.
const DefaultPasswordHashingAlgorithm = scryptHashAlgorithm

// PasswordHashingAlgorithms returns the supported algorithms for user password hashing.
func PasswordHashingAlgorithms() []string {
	return []string{scryptHashAlgorithm, pbkdf2HashAlgorithm}
}
