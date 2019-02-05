# 1. Take input files
# 2. Determine environment
#     1. If it's PMAC, might need to check for docker / singularity / modules and generate correct config
# 3. Start up Cromwell (in server mode)
# 4. Schedule job
# 5. Monitor job process
#     1. Return any errors that may occur
#     2. Give user running feedback
# 6. Collect output files
# 7. Shutdown cromwell server
import time
from engines.cromwell.main import Cromwell

c = Cromwell()
c.start()
print("started", c.process)
time.sleep(20)
c.stop()