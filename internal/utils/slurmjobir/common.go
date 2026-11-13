// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package slurmjobir

import (
	"errors"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

var (
	ErrorInsuffientPods     = errors.New("not enough pending pods to create external job")
	ErrorExternalJobInvalid = errors.New("not enough pending pods for created external job")
)

func ConvStrTo32(input string) (output *int32, err error) {
	out, err := strconv.ParseInt(input, 10, 32)
	if err != nil {
		return nil, err
	}
	retVal := int32(out)

	return &retVal, err
}

func ParseSlurmJobId(input string) int32 {
	out, err := strconv.ParseUint(input, 10, 32)
	if err != nil {
		return 0
	}
	return int32(out) //nolint:gosec // disable G115
}

// GetMemoryFromQuantity converts a memory quantity to MiB. Partial MiB are
// rounded up so the Slurm job never reserves less memory than the Pod may use.
func GetMemoryFromQuantity(quantity *resource.Quantity) int64 {
	val := quantity.Value()
	return (val + 1048576 - 1) / 1048576
}

func getFirstPod(p corev1.PodList) *corev1.Pod {
	if len(p.Items) > 0 {
		return &p.Items[0]
	}
	return nil
}
