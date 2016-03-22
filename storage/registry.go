package storage

import "fmt"

var (
	factories = map[string]repositoryFactory{}
)

// RepositoryFactory allows creation of repositories in a generic way.
type repositoryFactory struct {
	defaultConfigFunc    func() interface{}
	createRepositoryFunc func(interface{}) (Repository, error)
}

// AddSupportedRepository registers factory function to create repository with a given type name.
func AddSupportedRepository(
	repositoryType string,
	defaultConfigFunc func() interface{},
	createRepositoryFunc func(interface{}) (Repository, error)) {

	factories[repositoryType] = repositoryFactory{
		defaultConfigFunc:    defaultConfigFunc,
		createRepositoryFunc: createRepositoryFunc,
	}
}

// NewRepository creates new repository based on RepositoryConfiguration.
// The repository type must be previously registered using AddSupportedRepository.
func NewRepository(cfg RepositoryConfiguration) (Repository, error) {
	if factory, ok := factories[cfg.Type]; ok {
		return factory.createRepositoryFunc(cfg.Config)
	}

	return nil, fmt.Errorf("unknown repository type: %s", cfg.Type)
}
