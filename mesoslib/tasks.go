package mesoslib

import (
	"fmt"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/VoltFramework/volt/mesosproto"
	"github.com/golang/protobuf/proto"
)

type Volume struct {
	ContainerPath string `json:"container_path,omitempty"`
	HostPath      string `json:"host_path,omitempty"`
	Mode          string `json:"mode,omitempty"`
}

type Task struct {
	ID      string
	Command []string
	Image   string
	Volumes []*Volume
	Parameters []string
	EnvironmentVariables []string
}

func createTaskInfo(offer *mesosproto.Offer, resources []*mesosproto.Resource, task *Task) *mesosproto.TaskInfo {
	taskInfo := mesosproto.TaskInfo{
		Name: proto.String(fmt.Sprintf("volt-task-%s", task.ID)),
		TaskId: &mesosproto.TaskID{
			Value: &task.ID,
		},
		SlaveId:   offer.SlaveId,
		Resources: resources,
		Command:   &mesosproto.CommandInfo{},
	}

	fmt.Printf("SlaveId --- %v\n", offer.SlaveId);

	// Set value only if provided
	if task.Command[0] != "" {
		taskInfo.Command.Value = &task.Command[0]
	}

	// Set args only if they exist
	if len(task.Command) > 1 {
		taskInfo.Command.Arguments = task.Command[1:]
	}

	// Set the docker image if specified
	if task.Image != "" {
		taskInfo.Container = &mesosproto.ContainerInfo{
			Type: mesosproto.ContainerInfo_DOCKER.Enum(),
			Docker: &mesosproto.ContainerInfo_DockerInfo{
				Image: &task.Image,
			},
		}

		for _, v := range task.Volumes {
			var (
				vv   = v
				mode = mesosproto.Volume_RW
			)

			if vv.Mode == "ro" {
				mode = mesosproto.Volume_RO
			}

			taskInfo.Container.Volumes = append(taskInfo.Container.Volumes, &mesosproto.Volume{
				ContainerPath: &vv.ContainerPath,
				HostPath:      &vv.HostPath,
				Mode:          &mode,
			})
		}

		// No Error checking on Parameter Syntax
		// Assuming each element in array will be 
		// formatted "key value"
		fmt.Printf("Parameters  %v", task.Parameters);
		var parameters []*mesosproto.Parameter
		for _, element := range task.Parameters {
			params := strings.Split(element, " ");

			if len(params) == 2 {
				param_key := params[0];
				param_value := params[1];
				var params = &mesosproto.Parameter{ Key: &param_key, Value: &param_value}
				parameters = append(parameters, params);
			}
		}

		var environment_variables []* mesosproto.Environment_Variable
		fmt.Printf("ENV  %v\n\n", task.EnvironmentVariables);
		for _, element := range task.EnvironmentVariables {
			env_vars := strings.Split(element, " ");

			if len(env_vars) == 2 {
				env_var_name := env_vars[0];
				env_var_val := env_vars[1];
				env_variable := &mesosproto.Environment_Variable { Name: &env_var_name, Value: &env_var_val }
				environment_variables = append(environment_variables, env_variable);
				fmt.Printf("ENV VARS   %v\n", environment_variables);
			}
		}
		
		var environment = &mesosproto.Environment {Variables: environment_variables};
		taskInfo.Command.Environment = environment;
		taskInfo.Container.Docker.Parameters = parameters;

		taskInfo.Command.Shell = proto.Bool(true)
	}

	return &taskInfo

}

func (m *MesosLib) LaunchTask(offer *mesosproto.Offer, resources []*mesosproto.Resource, task *Task) error {
	m.Log.WithFields(logrus.Fields{"ID": task.ID, "command": task.Command, "offerId": offer.Id, "dockerImage": task.Image}).Info("Launching task...")

	fmt.Printf("Offer --- %v\n\n", offer);

	taskInfo := createTaskInfo(offer, resources, task)

	return m.send(&mesosproto.LaunchTasksMessage{
		FrameworkId: m.frameworkInfo.Id,
		Tasks:       []*mesosproto.TaskInfo{taskInfo},
		OfferIds: []*mesosproto.OfferID{
			offer.Id,
		},
		Filters: &mesosproto.Filters{},
	}, "mesos.internal.LaunchTasksMessage")
}

func (m *MesosLib) KillTask(ID string) error {
	m.Log.WithFields(logrus.Fields{"ID": ID}).Info("Killing task...")

	return m.send(&mesosproto.KillTaskMessage{
		FrameworkId: m.frameworkInfo.Id,
		TaskId: &mesosproto.TaskID{
			Value: &ID,
		},
	}, "mesos.internal.KillTaskMessage")
}
