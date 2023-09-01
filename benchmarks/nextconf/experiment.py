

# each query should produce a directory <run no> for which following files are stored:
#  <run no>/config.json --> configuration of that experiment
#  <run no>/env.json --> information about environment
#  <run no>/stats.json --> statistics about the experiment
#  <run no>/log.txt  --> the log corresponding to that experiment

class Benchmark:

    def __init__(self):
        self.name = 'unknown'
        pass

    def setup(self):
        pass

    def run(self):
        pass

    def shutdown(self):
        pass

    def get_stats(self):
        return {'name': self.name}