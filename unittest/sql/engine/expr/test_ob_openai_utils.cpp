#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include <gtest/gtest.h>
#include <string>

namespace oceanbase { namespace common {
namespace {
ObJsonObject *object(ObIAllocator &allocator, const char *text)
{
  ObJsonObject *result = nullptr;
  ObString input(text);
  EXPECT_EQ(OB_SUCCESS, ObAIFuncJsonUtils::get_json_object_form_str(allocator, input, result));
  return result;
}

TEST(AIFunctionUtils, RepeatedAndNestedPromptRendering)
{
  ObArenaAllocator allocator;
  ObJsonObject *prompt = object(allocator,
      R"({"template":"{0}/{0}/{1}","args":["中文🙂",{"template":"nested {0}","args":["x{0}"]}]})");
  ObString result;
  ASSERT_EQ(OB_SUCCESS, ObAIFuncPromptObjectUtils::replace_all_str_args_in_template(allocator, prompt, result));
  EXPECT_EQ(std::string("中文🙂/中文🙂/nested x{0}"), std::string(result.ptr(), result.length()));
  ObJsonArray *args = nullptr;
  ObJsonString *argument = nullptr;
  std::string long_text(8192, 'x');
  ObString value(long_text.size(), long_text.data());
  ObString text("{0}{0}{0}{0}{0}{0}{0}{0}");
  ASSERT_EQ(OB_SUCCESS, ObAIFuncJsonUtils::get_json_array(allocator, args));
  ASSERT_EQ(OB_SUCCESS, ObAIFuncJsonUtils::get_json_string(allocator, value, argument));
  ASSERT_EQ(OB_SUCCESS, args->append(argument));
  ASSERT_EQ(OB_SUCCESS, ObAIFuncPromptObjectUtils::construct_prompt_object(allocator, text, args, prompt));
  ASSERT_EQ(OB_SUCCESS, ObAIFuncPromptObjectUtils::replace_all_str_args_in_template(allocator, prompt, result));
  EXPECT_EQ(std::string(65536, 'x'), std::string(result.ptr(), result.length()));
}

TEST(AIFunctionUtils, PromptIndicesDoNotOverflow)
{
  ObArenaAllocator allocator;
  ObString result;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObAIFuncPromptObjectUtils::replace_all_str_args_in_template(allocator,
      object(allocator, R"({"template":"{999999999999999999999999999999}","args":["x"]})"), result));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObAIFuncPromptObjectUtils::replace_all_str_args_in_template(allocator,
      object(allocator, R"({"template":"{2}","args":["x"]})"), result));
}

TEST(AIFunctionUtils, EmbeddingIndicesRestoreInputOrder)
{
  ObArenaAllocator allocator;
  ObOpenAIUtils::ObOpenAIEmbed provider;
  ObIJsonBase *result = nullptr;
  ASSERT_EQ(OB_SUCCESS, provider.parse_output(allocator, object(allocator,
      R"({"data":[{"index":1,"embedding":[2,3]},{"index":0,"embedding":[0,1]}]})"), result));
  ASSERT_EQ(OB_SUCCESS, ObAIFuncUtils::validate_embedding_array(result, 2, 2));
  ObJsonArray *vectors = static_cast<ObJsonArray *>(result);
  EXPECT_EQ(0, static_cast<ObJsonArray *>(vectors->get_value(0))->get_value(0)->get_int());
  EXPECT_EQ(2, static_cast<ObJsonArray *>(vectors->get_value(1))->get_value(0)->get_int());
  EXPECT_EQ(OB_INVALID_DATA, provider.parse_output(allocator, object(allocator,
      R"({"data":[{"index":0,"embedding":[0,1]},{"index":0,"embedding":[2,3]}]})"), result));
  EXPECT_EQ(OB_INVALID_DATA, provider.parse_output(allocator, object(allocator,
      R"({"data":[{"index":1,"embedding":[0,1]}]})"), result));
  ASSERT_EQ(OB_SUCCESS, provider.parse_output(allocator, object(allocator,
      R"({"data":[{"index":0,"embedding":[0,"bad"]}]})"), result));
  EXPECT_EQ(OB_INVALID_DATA, ObAIFuncUtils::validate_embedding_array(result, 1, 2));
  EXPECT_EQ(OB_INVALID_DATA, ObAIFuncUtils::validate_embedding_array(result, 1, 3));
}

TEST(AIFunctionUtils, OllamaMapsConfigurationAndRejectsIncompleteResponses)
{
  ObArenaAllocator allocator;
  ObOllamaUtils::ObOllamaComplete provider;
  ObString model("local"), system("Return text"), prompt("hello");
  ObJsonObject *body = nullptr;
  ASSERT_EQ(OB_SUCCESS, provider.get_body(allocator, model, system, prompt,
      object(allocator, R"({"temperature":0.2,"max_tokens":8})"), body));
  EXPECT_FALSE(body->get_value("stream")->get_boolean());
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, body->get_value("options")->json_type());
  EXPECT_EQ(8, static_cast<ObJsonObject *>(body->get_value("options"))->get_value("num_predict")->get_int());
  EXPECT_EQ(nullptr, body->get_value("max_tokens"));
  ObIJsonBase *result = nullptr;
  EXPECT_EQ(OB_INVALID_DATA, provider.parse_output(allocator,
      object(allocator, R"({"done":false,"response":"partial"})"), result));
  EXPECT_EQ(OB_INVALID_DATA, provider.parse_output(allocator,
      object(allocator, R"({"done":true,"response":""})"), result));
}
} } }
