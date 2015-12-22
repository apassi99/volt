package mesoslib

import 	"fmt"

type Metrics struct {
	TotalCpus float64     `json:"total_cpus"`
	TotalMem  float64     `json:"total_mem"`
	TotalDisk float64     `json:"total_disk"`
	UsedCpus  float64     `json:"used_cpus"`
	UsedMem   float64     `json:"used_mem"`
	UsedDisk  float64     `json:"used_disk"`
	NumSlaves int         `json:"num_slaves"`
}

type SlaveMetric struct {
	SlaveIDs  [] string   `json:"slave_ids"`
}

func (m *MesosLib) SlaveIDs() (*SlaveMetric, error) {
	
	data, err := m.getMasterState()
	if err != nil {
		return nil, err
	}		

	var metrics SlaveMetric;

	var slave_ids [] string;

	for _, slave := range data.Slaves {
		slave_ids = append(slave_ids, slave.Id);
	}

	metrics.SlaveIDs = slave_ids;
	
	return &metrics, nil;
}

func (m *MesosLib) Metrics() (*Metrics, error) {
	data, err := m.getMasterState()
	if err != nil {
		return nil, err
	}

	var metrics Metrics

	for _, framework := range data.Frameworks {
		for _, task := range framework.Tasks {
			metrics.UsedMem += task.Resources.Mem
			metrics.UsedCpus += task.Resources.Cpus
			metrics.UsedDisk += task.Resources.Disk
		}
	}

	for _, slave := range data.Slaves {
		metrics.TotalMem += slave.Resources.Mem
		metrics.TotalCpus += slave.Resources.Cpus
		metrics.TotalDisk += slave.Resources.Disk
	}

	fmt.Printf("\n\nSlave Metrics %v\n\n", data.Slaves);

	metrics.NumSlaves = len(data.Slaves);

	return &metrics, nil
}
