"""
Implementation of the RUN scheduler as introduced in RUN: Optimal
Multiprocessor Real-Time Scheduling via Reduction to Uniprocessor
by Regnier et al.

RUN+ is a global multiprocessors scheduler for sporadic-preemptive-independent
tasks with implicit deadlines.
"""

from simso.core import Scheduler, Timer
from simso.schedulers.RUNPlusServer import EDFServer, TaskServer, DualServer, initialize_server_budget, is_reduction_tree_needed, \
    refresh_budget, select_jobs, select_jobs_part, add_job, get_child_tasks, toggle_servers_activation
from simso.schedulers import scheduler
from simso.utils.SchedLogger import sched_logger
from math import ceil

@scheduler("simso.schedulers.RUNPlus")
class RUNPlus(Scheduler):
    """
    RUN scheduler. The offline part is done here but the online part is mainly
    done in the SubSystem objects. The RUN object is a proxy for the
    sub-systems.
    """

    def init(self):
        """
        Initialization of the scheduler. This function is called when the
        system is ready to run.
        """
        self.subsystems = []  # List of all the sub-systems.
        self.available_cpus = self.processors[:]  # Not yet affected cpus.
        self.task_to_subsystem = {}  # map: Task -> SubSystem

        # Create the Task Servers. Those are the leaves of the reduction tree.
        list_servers = [TaskServer(task) for task in self.task_list]

        # map: Task -> TaskServer. Used to quickly find the servers to update.
        self.servers = dict(zip(self.task_list, list_servers))

        assert (sum([s.utilization for s in list_servers])
                <= len(self.processors)), "Load exceeds 100%!"

        # Instanciate the reduction tree and the various sub-systems.
        self.reduce_iterations(list_servers)

        # Initialize the budgets for all servers in reduction tree.
        self.initialize_budgets(list(set([s.parent for s in list_servers])))


    def add_idle_tasks(self, servers):
        """
        Create IdleTasks in order to reach 100% system utilization.
        """
        from collections import namedtuple
        # pylint: disable-msg=C0103
        IdleTask = namedtuple('IdleTask', ['utilization'])

        idle = len(self.processors) - sum([s.utilization for s in servers])
        for server in servers:
            if server.utilization < 1 and idle > 0:
                task = IdleTask(min(1 - server.utilization, idle))
                server.add_child(TaskServer(task))
                idle -= task.utilization
        while idle > 0:
            task = IdleTask(min(1, idle))
            server = EDFServer()
            server.add_child(TaskServer(task))
            idle -= task.utilization
            servers.append(server)

    def add_proper_subsystem(self, server, levels):
        """
        Create a proper sub-system from a unit server.
        """
        tasks_servers = get_child_tasks(server)
        subsystem_utilization = sum([t.utilization for t in tasks_servers])
        cpus = []
        while subsystem_utilization > 0:
            cpus.append(self.available_cpus.pop())
            subsystem_utilization -= 1

        subsystem = ProperSubsystem(self.sim, server, cpus, levels)
        sched_logger.tree_height = levels
        for server in tasks_servers:
            self.task_to_subsystem[server.task] = subsystem
        self.subsystems.append(subsystem)

    def remove_unit_servers(self, servers, levels):
        """
        Remove all the unit servers for a list and create a proper sub-system
        instead.
        """
        for server in servers:
            if server.utilization == 1:
                self.add_proper_subsystem(server, levels)
        servers[:] = [s for s in servers if s.utilization < 1]

    def reduce_iterations(self, servers):
        """
        Offline part of the RUN Scheduler. Create the reduction tree.
        """
        levels = 0
        servers = pack(servers, levels)
        # servers = force_paper_packing(servers, 5/7,levels)

        for server in servers:
            sched_logger.add_row("Server: %s" % (server.identifier))
            for task_server in server.children:
                if hasattr(task_server.task, "name"):
                    sched_logger.add_row(task_server.task.name)
                else:
                    sched_logger.add_row("Idle Task")
        sched_logger.add_row("\n\n")

        self.add_idle_tasks(servers)
        self.remove_unit_servers(servers, levels)

        while servers:
            levels += 1
            servers = pack(dual(servers), levels)

            self.remove_unit_servers(servers, levels)

    def on_activate(self, job):
        """
        Deal with a (real) task activation.
        """
        subsystem = self.task_to_subsystem[job.task]
        subsystem.update_budget()
        task_server = self.servers[job.task]
        task_server.is_active = True
        current_instant = self.sim.now()
        sched_logger.add_row("%s JOB ARRIVAL" % job.task.name)
        add_job(self.sim, job, self.servers[job.task])
        delay = task_server.parent.next_deadline - current_instant

        subsystem.create_deadline_event(self.servers[job.task].parent, delay)
        subsystem.resched(job.cpu)

    def on_terminated(self, job):
        """
        Deal with a (real) job termination.
        """
        subsystem = self.task_to_subsystem[job.task]
        self.task_to_subsystem[job.task].update_budget()
        subsystem.resched(job.cpu)

    def schedule(self, _):
        """
        This method is called by the simulator. The sub-systems that should be
        rescheduled are also scheduled.
        """
        decisions = []
        for subsystem in self.subsystems:
            if subsystem.to_reschedule:
                decisions += subsystem.schedule()

        return decisions

    def initialize_budgets(self, servers):
        """
        Initialize server at 0.
        """
        current_time = self.sim.now()
        for server in servers:
            initialize_server_budget(current_time, self.sim.cycles_per_ms, server)
            subsystem = self.task_to_subsystem[server.children[0].task]
            delay = server.next_deadline - self.sim.now()
            subsystem.timer = Timer(subsystem.sim, ProperSubsystem.deadline_event,
                            (subsystem, server, subsystem.processors[0]),
                            delay, in_ms=False, cpu=subsystem.processors[0])
            subsystem.timer.start()
            subsystem.resched(subsystem.processors[0])
    sched_logger.add_row("BUDGETS INITIALIZATION COMPLETE")


def pack(servers, level = -1):
    """
    Create a list of EDF Servers by packing servers. Currently use a
    First-Fit but the original article states they used a Worst-Fit packing
    algorithm. According to the article, a First-Fit should also work.
    """
    packed_servers = []
    server_identifier = 1
    for server in servers:
        for p_server in packed_servers:
            if p_server.utilization + server.utilization <= 1:
                p_server.add_child(server)
                break
        else:
            p_server = EDFServer()
            p_server.identifier = "σ%d%d" % (level, server_identifier)
            server_identifier += 1
            p_server.add_child(server)
            packed_servers.append(p_server)

    return packed_servers

def force_specific_packing(servers, target_server_utilization, level = -1):
    packed_servers = []
    server_identifier = 1
    for server in servers:
        for p_server in packed_servers:
            if p_server.utilization + server.utilization <= target_server_utilization:
                p_server.add_child(server)
                break
        else:
            p_server = EDFServer()
            p_server.identifier = "σ%d%d" % (level, server_identifier)
            server_identifier += 1
            p_server.add_child(server)
            packed_servers.append(p_server)

    return packed_servers

def dual(servers):
    """
    From a list of servers, returns a list of corresponding DualServers.
    """
    dual_servers = [DualServer(s) for s in servers]
    for server in dual_servers:
        server.identifier = "%s*" % (server.child.identifier)
    return dual_servers


class ProperSubsystem(object):
    """
    Proper sub-system. A proper sub-system is the set of the tasks belonging to
    a unit server (server with utilization of 1) and a set of processors.
    """

    def __init__(self, sim, root, processors, levels):
        self.sim = sim
        self.root = root
        self.processors = processors
        self.virtual = []
        self.last_update = 0
        self.to_reschedule = False
        self.timer = None
        self.levels = levels
        self.components_quantity = 0
        self.components_roots = []
        self.is_reduction_tree_active = False
        self.define_levels(self.root, levels)
        sched_logger.add_row("\n\n")
        self.define_components(self.root)

    def update_budget(self):
        """
        Update the budget of the servers.
        """
        time_since_last_update = self.sim.now() - self.last_update
        if time_since_last_update > 0:
            for server in self.virtual:
                server.budget -= time_since_last_update
                sched_logger.add_row("CURRENT BUDGET At %d SERVER %s BUDGET %d" % (
                    self.sim.now(),
                    server.identifier,
                    server.budget
                ))
            self.last_update = self.sim.now()

    def resched(self, cpu):
        """
        Plannify a scheduling decision on processor cpu. Ignore it if already
        planned.
        """
        if not self.to_reschedule:
            self.to_reschedule = True
            cpu.resched()

    def virtual_event(self, cpu, target_server):
        """
        Virtual scheduling event. Happens when a virtual job terminates.
        """
        self.update_budget()
        if target_server.budget == 0:
            self.resched(cpu)

    def deadline_event(self, server, cpu):
        """
        Deadline event. Happens when a server reaches its deadline.
        """
        current_time = self.sim.now()
        if server.last_updated_by_deadline_event != current_time:
            self.update_budget()
            refresh_budget(server, current_time, self.sim.cycles_per_ms)
            # TODO: verify repetition
            if server.last_updated_by_deadline_event == current_time:
                sched_logger.add_row("UPDATED BY DEADLINE OF %s AT %d" % (
                    server.identifier,
                    current_time
                ))
            if server.next_deadline > current_time:
                self.create_deadline_event(server, delay=server.next_deadline - current_time)
            self.resched(cpu)

    def schedule(self):
        """
        Schedule this proper sub-system.
        """
        self.to_reschedule = False
        decision = []
        jobs = []

        self.virtual = []
        current_time = self.sim.now()
        sched_logger.add_row("At time %d" % current_time)
        current_red_tree_status = self.is_reduction_tree_active
        self.is_reduction_tree_active = is_reduction_tree_needed(self.root)
        if self.is_reduction_tree_active:
            for component_root in reversed(self.components_roots):
                self.checks_component_activation(component_root, current_red_tree_status)
                jobs += self.get_component_jobs(component_root)
        else:
            jobs = select_jobs_part(self.root, self.virtual)

        sched_logger.add_row("\nALL TASKS CHOSEN")
        if len(jobs) > len(self.processors):
            sched_logger.add_row("ERROR n > m AT: %d" % current_time)
        for job in jobs:
            sched_logger.add_row("CHOSEN T%s" % job.task.identifier)

        sched_logger.add_row("\n\n")
        virtual_positive = list(filter(lambda s: s.budget > 0, self.virtual))
        if len(virtual_positive) > 0:
            possible_empty = min(self.virtual, key=lambda s: s.budget)
            if possible_empty.budget > 0:
                self.timer = Timer(self.sim, ProperSubsystem.virtual_event,
                                (self, self.processors[0], possible_empty), possible_empty.budget,
                                cpu=self.processors[0], in_ms=False)
                self.timer.start()

        cpus = []
        for cpu in self.processors:
            if cpu.running in jobs:
                jobs.remove(cpu.running)
            else:
                cpus.append(cpu)

        for cpu in cpus:
            if jobs:
                decision.append((jobs.pop(), cpu))
            else:
                decision.append((None, cpu))

        return decision

    def checks_component_activation(self, component_root, last_red_tree_status):
        """
        Checks if is necessary to activate the component.
        """
        current_time = self.sim.now()
        sched_logger.add_row("\nSCHED FOR ROOT %s" % component_root.identifier)
        is_component_active = component_root.is_active
        toggle_servers_activation(
            component_root,
            self.sim.now(),
            self.sim.cycles_per_ms,
            self.root
        )
        if last_red_tree_status == False or (component_root.is_active and not is_component_active):
            # Set deadline event for all children
            task_servers = component_root.get_active_tasks_servers()
            for task_server in task_servers:
                self.create_deadline_event(
                    task_server.parent,
                    delay=task_server.parent.next_deadline - current_time
                )

    def get_component_jobs(self, component_root):
        """
        Get the jobs for the component.
        """
        jobs = []
        if component_root == self.root:
            jobs = select_jobs(component_root, self.virtual)
        elif not component_root.is_active:
            jobs = select_jobs_part(component_root, self.virtual)

        return jobs

    def create_deadline_event(self, target_server, delay):
        """
        Create deadline event for server.
        """
        self.timer = Timer(self.sim, ProperSubsystem.deadline_event,
                    (self, target_server, self.processors[0]),
                    delay, in_ms=False, cpu=self.processors[0])
        self.timer.start()

    def define_components(self, server, component = 1):
        """
        Define components of tree.
        """
        if not server.is_dual and not hasattr(server, "children"):
            return

        if server.is_dual:
            current_component = component
            if not server.child.children[0].task:
                server.child.add_component(component)
                self.components_quantity += 1
                current_component += self.components_quantity
            else:
                current_component = component

            self.define_components(server.child, current_component)
        else:
            if server.level != 0:
                self.components_roots.append(server)
                server.set_component_leaves()
            for child in server.children:
                self.define_components(child, component)

        server.add_component(component)


    def define_levels(self, server, level):
        """
        Define the levels of server.
        """
        if isinstance(server, TaskServer):
            server.set_level(-1)
            if hasattr(server.task, 'name'):
                server.identifier = "σ" + server.task.name
            else:
                server.identifier = "IDLE"
            sched_logger.add_row("ID: %s, DUAL: %s, UTIL: %s, LEVEL: %d" % (
                server.identifier,
                server.is_dual,
                server.utilization,
                server.level
            ))
            return
        else:
            server.set_level(level)
            sched_logger.add_row("ID: %s, DUAL: %s, UTIL: %s, LEVEL: %d" % (
                server.identifier,
                server.is_dual,
                server.utilization,
                server.level
            ))

        if server.is_dual:
            self.define_levels(server.child, level)
        else:
            server.set_task_servers()
            for children in server.children:
                self.define_levels(children, level - 1)
