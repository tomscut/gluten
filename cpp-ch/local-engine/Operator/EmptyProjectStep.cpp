#include "EmptyProjectStep.h"
#include <Common/CHUtil.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace local_engine
{

class EmptyProject : public DB::IProcessor
{
public:
    explicit EmptyProject(const DB::Block & input_)
        : DB::IProcessor({input_}, {BlockUtil::buildRowCountHeader()})
    { }

    ~EmptyProject() override = default;

    String getName() const override { return "EmptyProject"; }

    Status prepare() override
    {
        auto & output = outputs.front();
        auto & input = inputs.front();
        if (output.isFinished())
        {
            input.close();
            return Status::Finished;
        }
        if (has_output)
        {
            if (output.canPush())
            {
                output.push(std::move(output_chunk));
                has_output = false;
            }
            return Status::PortFull;
        }

        if (has_input)
        {
            return Status::Ready;
        }

        if (input.isFinished())
        {
            output.finish();
            return Status::Finished;
        }

        input.setNeeded();
        if (input.hasData())
        {
            output_chunk = input.pull(true);
            output_chunk = BlockUtil::buildRowCountChunk(output_chunk.getNumRows());
            has_input = true;
            return Status::Ready;
        }
        return Status::NeedData;
    }

    void work() override
    {
        has_input = false;
        has_output = true;
    }
private:
    DB::Chunk output_chunk;
    bool has_input = false;
    bool has_output = false;
};

static DB::ITransformingStep::Traits getTraits()
{
    return DB::ITransformingStep::Traits{
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }};
}

EmptyProjectStep::EmptyProjectStep(const DB::DataStream & input_stream_)
    : ITransformingStep(input_stream_, BlockUtil::buildRowCountHeader(), getTraits())
{
}

void EmptyProjectStep::transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings & /*settings*/)
{
    auto build_transform = [&](DB::OutputPortRawPtrs outputs)
    {
        DB::Processors new_processors;
        for (auto & output : outputs)
        {
            auto op = std::make_shared<EmptyProject>(output->getHeader());
            new_processors.push_back(op);
            DB::connect(*output, op->getInputs().front());
        }
        return new_processors;
    };
    pipeline.transform(build_transform);
}

void EmptyProjectStep::describePipeline(DB::IQueryPlanStep::FormatSettings & settings) const
{
    if (!processors.empty())
        DB::IQueryPlanStep::describePipeline(processors, settings);
}

void EmptyProjectStep::updateOutputStream()
{
    createOutputStream(input_streams.front(), BlockUtil::buildRowCountHeader(), getDataStreamTraits());
}
}
