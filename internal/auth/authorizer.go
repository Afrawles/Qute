package auth

import (
	"fmt"

	"github.com/casbin/casbin/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Authorizer struct {
	enforcer *casbin.Enforcer
}

func New(model, policy string) (*Authorizer, error) {
	e, err := casbin.NewEnforcer(model, policy)
	if err != nil {
		return nil, err
	}

	return &Authorizer{
		enforcer: e,
	}, nil
}

func (a *Authorizer) Authorize(subject, object, action string) error {
	ok, err := a.enforcer.Enforce(subject, object, action)
	if err != nil {
		return err
	}

	if !ok {
		st := status.New(
			codes.PermissionDenied,
			fmt.Sprintf("%s not permitted to %s %s", subject, action, object),
		)
		return st.Err()
	}

	return nil
}
