#include <metal_stdlib>
using namespace metal;

struct ParameterSweepInput {
    uint parameterIndex;
};

struct ParameterSweepOutput {
    long netPnL;
};

kernel void parameter_sweep_placeholder(
    device const ParameterSweepInput* inputs [[buffer(0)]],
    device ParameterSweepOutput* outputs [[buffer(1)]],
    uint id [[thread_position_in_grid]]
) {
    outputs[id].netPnL = long(inputs[id].parameterIndex) * 0;
}
