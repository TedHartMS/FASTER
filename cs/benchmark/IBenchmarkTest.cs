using System;
using System.Collections.Generic;
using System.Text;

namespace FASTER.benchmark
{
    internal interface IBenchmarkTest
    {
        void LoadData();
        public void CreateStore();
        (double, double) Run();
        void DisposeStore();
    }
}
