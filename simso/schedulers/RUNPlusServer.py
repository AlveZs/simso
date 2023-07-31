"""
This module is part of the RUN implementation (see RUN.py).
"""

from fractions import Fraction
from decimal import Decimal
from simso.utils.SchedLogger import sched_logger


class _Server(object):
    """
    Abstract class that represents a Server.
    """
    next_id = 1

    def __init__(self, is_dual, task=None):
        self.parent = None
        self.is_dual = is_dual
        self.utilization = Fraction(0, 1)
        self.task = task
        self.job = None
        self.deadlines = [0]
        self.budget = 0
        self.next_deadline = 0
        self.components = []
        self.threshold = 0
        self.level = -1
        self.identifier = _Server.next_id
        self.is_active = False
        self.last_updated_by_deadline_event = -1
        self.budget_excess = 0
        _Server.next_id += 1
        if task:
            if hasattr(task, 'utilization'):
                self.utilization += task.utilization
            else:
                self.utilization += (
                    Fraction(Decimal(str(task.wcet))) /
                    Fraction(Decimal(str(task.deadline))
                ))

    def add_deadline(self, current_instant, deadline):
        """
        Add a deadline to this server.
        """
        self.deadlines.append(deadline)

        self.deadlines = [d for d in self.deadlines if d > current_instant]
        self.next_deadline = min(self.deadlines)

    def refresh_deadlines(self, current_instant, cycles_per_ms):
        """
        Update the deadlines for the server.
        """
        self.deadlines = []

        if self.is_dual:
            self.next_deadline = self.child.next_deadline
        elif self.task:
            if self.task.job:
                self.next_deadline = self.task.job.absolute_deadline_cycles
            else:
                self.next_deadline = 0
        else:
            for server in self.children:
                if server.task:
                    if hasattr(server.task, "deadline"):
                        self.deadlines.append(calculate_horizon(
                            server,
                            current_instant,
                            cycles_per_ms
                        ))
                else:
                    self.deadlines.append(server.next_deadline)
            self.deadlines = [d for d in self.deadlines if d > current_instant]
            if len(self.deadlines) > 0:
                self.next_deadline = min(self.deadlines)
            else:
                self.next_deadline = 0

    def add_component(self, component):
        """
        Add the server to a component.
        """
        self.components.append(component)

    def create_job(self, current_instant):
        """
        Replenish the budget.
        """
        self.budget = int(self.utilization * (self.next_deadline -
                          current_instant))

    def compute_dual_budget(self, current_instant):
        """
        Compute dual budget.
        """
        self.budget = int(self.utilization * (self.next_deadline -
                          current_instant))

    def compute_budget(self, current_instant):
        """
        Compute initial budget.
        """
        active_server_tasks = list(filter(
            lambda server: current_instant < server.next_deadline,
            self.children
        ))

        active_tasks_utilization = sum(map(lambda server:
            server.utilization,
            active_server_tasks
        ))

        self.budget = int(active_tasks_utilization * (self.next_deadline -
                          current_instant))

    def replenish_budget(self, task_utilization, current_instant):
        """
        Replenish the budget.
        """
        self.budget += int(task_utilization * (self.next_deadline -
                          current_instant))

    def set_level(self, level):
        """
        Set the lever of server.
        """
        self.level = level


class TaskServer(_Server):
    """
    A Task Server is a Server that contains a real Task.
    """
    def __init__(self, task):
        super(TaskServer, self).__init__(False, task)


class EDFServer(_Server):
    """
    An EDF Server is a Server with multiple children scheduled with EDF.
    """
    def __init__(self):
        super(EDFServer, self).__init__(False)
        self.children = []
        self.task_servers = []
        self.component_leaves = []

    def add_child(self, server):
        """
        Add a child to this EDFServer (used by the packing function).
        """
        self.children.append(server)
        self.utilization += server.utilization
        server.parent = self

    def set_task_servers(self):
        """
        Define the tasks servers of the EDF server.
        """
        self.task_servers = get_real_child_tasks(self)

    def set_component_leaves(self):
        """
        Define the leaves of the component that the server is a part of.
        """
        self.component_leaves = [dual_server.child for dual_server in self.children]

    def get_active_tasks_servers(self):
        """
        Return the active task servers.
        """
        return list(filter(lambda task_server: task_server.is_active, self.task_servers))


class DualServer(_Server):
    """
    A Dual server is the opposite of its child.
    """
    def __init__(self, child):
        super(DualServer, self).__init__(True)
        self.child = child
        child.parent = self
        self.utilization = 1 - child.utilization


def add_job(sim, job, server):
    """
    Recursively update the deadlines of the parents of server.
    """
    server.job = job
    current_time = sim.now()

    while server:
        server.budget_excess = 0
        is_replenish = (
            current_time != 0 and
            server.last_updated_by_deadline_event != current_time and
            server.next_deadline > current_time
        )
        calculate_budget(
            server,
            current_time,
            sim.cycles_per_ms,
            is_replenish,
            Fraction(Decimal(str(job.wcet))) / Fraction(Decimal(str(job.deadline)))
        )
        server = server.parent

def select_jobs_part(server, virtual):
    """
    Select jobs for partitioned scheduling.
    """
    active_servers = [l for l in get_component_leaves(server) if l.budget > 0]

    jobs = []
    for target_server in active_servers:
        task_servers = ([
            s for s in target_server.children
            if hasattr(s.task, "deadline") and s.task.is_active()
        ])
        if len(task_servers) > 0:
            chosen_server = min(task_servers, key=lambda s: s.next_deadline)
            sched_logger.add_row("CHOSEN PART %s" % chosen_server.identifier)
            sched_logger.add_row("CHOSEN PART %s" % chosen_server.parent.identifier)
            virtual.append(chosen_server)
            virtual.append(chosen_server.parent)
            if chosen_server.job and chosen_server.job.is_active():
                jobs.append(chosen_server.job)

    return jobs


def select_jobs(server, virtual, execute=True):
    """
    Select the jobs that should run according to RUN. The virtual jobs are
    appended to the virtual list passed as argument.
    """
    jobs = []
    if execute and server.budget > 0:
        sched_logger.add_row("CHOSEN %s" % server.identifier)
        virtual.append(server)

    if server.task:
        if execute and server.budget > 0 and server.job.is_active():
            jobs.append(server.job)
    else:
        if server.is_dual:
            if server.child.budget == 0:
                jobs += select_jobs(server.child, virtual, False)
            else:
                jobs += select_jobs(server.child, virtual, not execute)
        else:
            active_servers = [s for s in server.children if s.is_active and s.budget > 0]
            if active_servers:
                min_server = min(active_servers, key=lambda s: s.next_deadline)
            else:
                min_server = None

            for child in server.children:
                if child.is_active:
                    jobs += select_jobs(child, virtual,
                                        execute and child is min_server)

    return jobs

def get_component_leaves(server):
    """
    Get the servers in component leaves.
    """
    leaves_servers = list(set([s.parent for s in server.task_servers]))

    return leaves_servers

def checks_tasks_activation(tasks_servers):
    """
    Checks if task is active.
    """
    for task_server in tasks_servers:
        task_server.is_active = task_server.budget > 0

def checks_edf_server_activation(server):
    """
    Checks if server is active.
    """
    server.is_active = server.budget > 0

def is_reduction_tree_needed(root_server):
    """
    Checks if the reduction tree is needed.
    """
    leaves_servers = get_component_leaves(root_server)
    checks_tasks_activation(root_server.task_servers)
    for edf_server in leaves_servers:
        checks_edf_server_activation(edf_server)

    active_servers = list(filter(lambda l: l.is_active, leaves_servers))
    servers_utilization = sum(server.utilization for server in leaves_servers)

    is_active = len(active_servers) > servers_utilization

    if not is_active:
        sched_logger.add_row("REDUNDANT TREE\n")
        disable_tree(root_server)

    return is_active

def toggle_servers_activation(server, current_instant, cycles_per_ms, tree_root):
    """
    Change the status of server.
    """
    current_status = server.is_active
    update_servers_status(server)

    leaves_servers = list(set([s.parent for s in server.task_servers]))

    active_servers = list(filter(lambda l: l.is_active, leaves_servers))
    servers_utilization = sum(server.utilization for server in leaves_servers)
    is_active = len(active_servers) > servers_utilization

    if is_active:
        sched_logger.add_row("\nRED. TREE ACTIVE\n")
    else:
        sched_logger.add_row("\nRED. TREE DISABLED\n")

    server.is_active = is_active
    if tree_root.level == 1 or server != tree_root:
        for child in server.children:
            child.is_active = is_active

    if server != tree_root:
        curr_server = server.parent
        while curr_server != tree_root:
            curr_server.is_active = is_active
            curr_server = curr_server.parent

    if (current_status == False and is_active):
        sched_logger.add_row("\nRED. TREE REACTIVATED\n")
        reactivate_tree(server, current_instant, cycles_per_ms)

    return is_active

def update_servers_status(component_root):
    """
    Update the status of all servers in components.
    """
    task_servers = component_root.task_servers
    checks_tasks_activation(task_servers)
    for edf_server in component_root.component_leaves:
        checks_edf_server_activation(edf_server)

def reactivate_tree(component_root, current_instant, cycles_per_ms):
    """
    Reactivate the reduction tree.
    """
    sched_logger.add_row("ADJUST FOR COMPONENT WITH ROOT %s" % (component_root.identifier))
    adjust_reactivated_tree_budget(
        current_instant,
        cycles_per_ms,
        component_root.budget,
        component_root
    )

    for s in component_root.component_leaves:
        curr_server = s.parent
        while curr_server != component_root:
            adjust_reactivated_tree_budget(
                current_instant,
                cycles_per_ms,
                curr_server.budget,
                curr_server
            )
            curr_server = curr_server.parent

def adjust_reactivated_tree_budget(current_time, cycles_per_ms, current_budget, server):
    """
    Adjust the budget of reactivated tree.
    """
    total_budget = server.budget_excess + current_budget
    sched_logger.add_row("%s BUDGET BEFORE %d + %d EXCESS = %d, CURRENT BUDGET %d" % (
                         server.identifier,
                         current_budget,
                         server.budget_excess,
                         total_budget,
                         server.budget
    ))
    server.refresh_deadlines(current_time, cycles_per_ms)
    server.create_job(current_time)
    server.budget = min(server.budget, total_budget)
    if total_budget > server.budget:
        server.budget_excess = total_budget - server.budget
    else:
        server.budget_excess = 0
    log_server(server, current_time)

def get_child_tasks(server):
    """
    Get the tasks scheduled by this server.
    """
    if server.task:
        return [server]
    else:
        if server.is_dual:
            return get_child_tasks(server.child)
        else:
            tasks = []
            for child in server.children:
                tasks += get_child_tasks(child)
            return tasks

def get_real_child_tasks(server):
    """
    Get the tasks scheduled by this server.
    """
    if server.task:
        if hasattr(server.task, 'deadline'):
            return [server]
        else:
            return []
    else:
        if server.is_dual:
            return get_real_child_tasks(server.child)
        else:
            tasks = []
            for child in server.children:
                tasks += get_real_child_tasks(child)
            return tasks

def refresh_budget(server, current_instant, cycles_per_ms):
    """
    Refresh the budget by deadline event.
    """
    checks_tasks_activation(server.children)
    checks_edf_server_activation(server)
    active_task_servers = [s for s in server.children if s.is_active]
    if len(active_task_servers) > 0:
        while server:
            server.budget_excess = 0
            server.refresh_deadlines(current_instant, cycles_per_ms)
            if server.level == -1:
                if server.job:
                    server.create_job(current_instant)
            elif server.is_dual:
                server.create_job(current_instant)
            else:
                server.compute_budget(current_instant)
            log_server(server,current_instant)
            server.last_updated_by_deadline_event = current_instant
            server = server.parent

def print_server_info(server, current_instant):
    """
    Auxiliar method to print server info.
    """
    print("At instant:", current_instant)
    if server.is_dual:
        print("Dual of %s" % (server.child.identifier))
    print("SERVER ID %s Dual: %s Budget: %d" % (server.identifier, server.is_dual, server.budget))

def log_server(server, current_instant):
    """
    Add server info to log.
    """
    sched_logger.add_row("At %d SERVER %s DEADLINE %s BUDGET %s" % (
        current_instant,
        server.identifier,
        server.next_deadline,
        server.budget
    ))

def calculate_horizon(task_server, current_instant, cycles_per_ms):
    """
    Calculate horizon.
    """
    horizon = -1
    if hasattr(task_server.task, "deadline"):
        if task_server.task.job:
            current_deadline = (
                (task_server.task.job.activation_date + task_server.task.deadline) *
                cycles_per_ms
            )
            if current_deadline > current_instant:
                horizon = current_deadline
            else:
                horizon = current_instant + (task_server.task.deadline * cycles_per_ms)
        else:
            horizon = current_instant + (task_server.task.deadline * cycles_per_ms)
    return horizon

def calculate_budget(
    server,
    current_time,
    cycles_per_ms,
    is_replenish,
    task_utilization = 0
):
    """
    Calculate the budget replenish or update.
    """
    if not is_replenish:
        server.refresh_deadlines(current_time, cycles_per_ms)

    if server.task:
        if server.job:
            server.create_job(current_time)
    elif server.is_dual:
        if is_replenish and server.budget < 0 or not is_replenish:
            server.create_job(current_time)
    else:
        if is_replenish:
            sched_logger.add_row("REPLENISH")
            server.replenish_budget(
                task_utilization,
                current_time
            )
        else:
            server.compute_budget(current_time)

    log_server(server, current_time)

def initialize_task_server_budget(server,current_time,cycles_per_ms):
    """
    Initialize the budgets at time 0
    """
    server.refresh_deadlines(current_time, cycles_per_ms)

    if server.job:
        server.create_job(current_time)
    log_server(server, current_time)

def set_component_nearest_deadline(component_root, current_time):
    """
    Set component nearest deadline for deadline sharing.
    """
    task_servers = component_root.task_servers
    checks_tasks_activation(task_servers)
    active_servers = list(filter(lambda ts: ts.is_active and ts.next_deadline > current_time, task_servers))
    if len(active_servers) > 0:
        nearest_deadline = min(active_servers, key=lambda s: s.next_deadline).next_deadline
        for task_server in task_servers:
            dual_server = task_server.parent
            dual_server.next_deadline = nearest_deadline
            dual_server.parent.next_deadline = nearest_deadline

def get_tree_nearest_deadline(root, current_time, cycles_per_ms):
    """
    Get the entire tree nearest deadline for deadline sharing.
    """
    deadlines = []
    for task_server in root.task_servers:
        if task_server.task:
            if hasattr(task_server.task, "deadline"):
                deadlines.append(calculate_horizon(task_server, current_time, cycles_per_ms))
    deadlines = [d for d in deadlines if d > current_time]
    if len(deadlines) > 0:
        return min(deadlines)
    else:
        return 0

def set_deadline_for_all_tree(server, deadline):
    """
    Set same deadline for all tree (deadline sharing).
    """
    if isinstance(server, TaskServer):
        return

    server.next_deadline = deadline
    if server.is_dual:
        set_deadline_for_all_tree(server.child, deadline)
    else:
        for child in server.children:
            set_deadline_for_all_tree(child, deadline)

def disable_tree(server):
    """
    Disable all servers in tree.
    """
    if not server.is_dual and server.level == 0:
        return
    server.is_active = False
    if server.is_dual:
        disable_tree(server.child)
    else:
        for child in server.children:
            disable_tree(child)

def initialize_server_budget(current_time, cycles_per_ms, server):
    """
    Recursively update the deadlines of the parents of server.
    """
    while server:
        server.refresh_deadlines(current_time, cycles_per_ms)
        if server.parent is None or server.task:
            server.create_job(current_time)
        else:
            if server.is_dual:
                server.compute_dual_budget(current_time)
            else:
                server.compute_budget(current_time)

        log_server(server, current_time)
        server = server.parent
