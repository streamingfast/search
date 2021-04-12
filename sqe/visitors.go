package sqe

import (
	"context"
	"errors"
)

var ErrStopVisit = errors.New("stop")

type DepthFirstVisitor struct {
	callback func(ctx context.Context, expr Expression) error
	stopped  bool
}

func NewDepthFirstVisitor(callback func(ctx context.Context, expr Expression) error) *DepthFirstVisitor {
	return &DepthFirstVisitor{callback: callback}
}

func (v *DepthFirstVisitor) Visit_And(ctx context.Context, e *AndExpression) error {
	return v.visit_binary(ctx, e, e.Children)
}

func (v *DepthFirstVisitor) Visit_Or(ctx context.Context, e *OrExpression) error {
	return v.visit_binary(ctx, e, e.Children)
}

func (v *DepthFirstVisitor) visit_binary(ctx context.Context, parent Expression, children []Expression) error {
	if stop, err := v.executeCallback(ctx, parent); stop {
		return err
	}

	for _, child := range children {
		err := child.Visit(ctx, v)
		if v.stopped || err != nil {
			return err
		}
	}
	return nil
}

func (v *DepthFirstVisitor) Visit_Parenthesis(ctx context.Context, e *ParenthesisExpression) error {
	if stop, err := v.executeCallback(ctx, e); stop {
		return err
	}

	return e.Child.Visit(ctx, v)
}

func (v *DepthFirstVisitor) Visit_Not(ctx context.Context, e *NotExpression) error {
	if stop, err := v.executeCallback(ctx, e); stop {
		return err
	}

	return e.Child.Visit(ctx, v)
}

func (v *DepthFirstVisitor) Visit_SearchTerm(ctx context.Context, e *SearchTerm) error {
	if stop, err := v.executeCallback(ctx, e); stop {
		return err
	}

	return nil
}

func (v *DepthFirstVisitor) executeCallback(ctx context.Context, e Expression) (stop bool, err error) {
	if v.stopped == true {
		return true, nil
	}

	if err := v.callback(ctx, e); err != nil {
		if err == ErrStopVisit {
			v.stopped = true
			return true, nil
		} else {
			v.stopped = true
			return true, err
		}
	}

	return false, nil
}
