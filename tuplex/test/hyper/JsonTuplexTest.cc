//
// Created by leonhard on 9/29/22.
//

#include "HyperUtils.h"
#include "MimeType.h"

#include <physical/experimental/JsonHelper.h>
#include <physical/experimental/JSONParseRowGenerator.h>

#include <jit/LLVMOptimizer.h>

#include <physical/execution/JsonReader.h>

class JsonTuplexTest : public HyperPyTest {};


namespace tuplex {
    void explainJsonMatch(const std::string& line,
                          const python::Type& type,
                          const std::vector<std::string>& columns,
                          bool fill_in_null_for_first_level,
                          std::ostream& os = std::cout) {
        using namespace std;
        os<<"Hello world"<<endl;

        // parse row
        auto rows = parseRowsFromJSON(line, nullptr, false, true);
        if(rows.empty())
            return;

        auto row = rows.front();

        // go through first level and check types
        auto root_struct_type = row.getType(0);
        assert(root_struct_type.isStructuredDictionaryType());
        auto root_pairs = root_struct_type.get_struct_pairs();

        assert(type.isTupleType() && type.parameters().size() == columns.size());

        // go through columns and check
        for(unsigned i = 0; i < columns.size(); ++i) {
            os<<"column "<<i<<": ";


            auto target_type = type.parameters()[i];

            // get type (or null if not found)
            auto name = columns[i];
            auto it = std::find_if(root_pairs.begin(), root_pairs.end(), [&name](const python::StructEntry& entry) {
                return str_value_from_python_raw_value(entry.key) == name;
            });

            auto col_type = it == root_pairs.end() ? python::Type::NULLVALUE : it->valueType;

            std::string match;
            // upcast? match?
            if(python::canUpcastType(col_type, target_type)) {
                match = "ok";
                if(it == root_pairs.end()) // was it a null substitution?
                    match += " (auto-null)";
            } else {
                match = "failed";
            }
            os<<match;
            os<<endl;
        }

    }
}

TEST_F(JsonTuplexTest, ExplainJsonMismatch) {
    using namespace tuplex;
    using namespace std;

    auto type_str = "(Option[str],Option[boolean],Option[Struct[(str,'avatar_url'=>str),(str,'display_login'=>str),(str,'gravatar_id'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'url'=>str)]],Option[str],Option[Struct[(str,'action'=>str),(str,'before'=>str),(str,'comment'=>Struct[(str,'_links'=>Struct[(str,'self'=>Struct[(str,'href'=>str)]),(str,'html'=>Struct[(str,'href'=>str)]),(str,'pull_request'=>Struct[(str,'href'=>str)])]),(str,'author_association'=>str),(str,'body'=>str),(str,'commit_id'=>str),(str,'created_at'=>str),(str,'diff_hunk'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'in_reply_to_id'=>i64),(str,'issue_url'=>str),(str,'line'=>Option[i64]),(str,'node_id'=>str),(str,'original_commit_id'=>str),(str,'original_line'=>i64),(str,'original_position'=>i64),(str,'original_start_line'=>Option[i64]),(str,'path'=>Option[str]),(str,'performed_via_github_app'=>null),(str,'position'=>Option[i64]),(str,'pull_request_review_id'=>i64),(str,'pull_request_url'=>str),(str,'reactions'=>Struct[(str,'url'=>str),(str,'total_count'=>i64),(str,'+1'=>i64),(str,'-1'=>i64),(str,'laugh'=>i64),(str,'hooray'=>i64),(str,'confused'=>i64),(str,'heart'=>i64),(str,'rocket'=>i64),(str,'eyes'=>i64)]),(str,'side'=>str),(str,'start_line'=>Option[i64]),(str,'start_side'=>Option[str]),(str,'updated_at'=>str),(str,'url'=>str),(str,'user'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>boolean),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)])]),(str,'commits'=>List[Struct[(str,'author'->Struct[(str,'email'->str),(str,'name'->str)]),(str,'distinct'=>boolean),(str,'message'->str),(str,'sha'->str),(str,'url'->str)]]),(str,'desc'=>Option[str]),(str,'description'=>Option[str]),(str,'distinct_size'=>i64),(str,'download'=>Struct[(str,'name'=>str),(str,'created_at'=>str),(str,'size'=>i64),(str,'content_type'=>str),(str,'url'=>str),(str,'download_count'=>i64),(str,'id'=>i64),(str,'description'=>str),(str,'html_url'=>str)]),(str,'forkee'=>Struct[(str,'allow_forking'=>boolean),(str,'archive_url'=>str),(str,'archived'=>boolean),(str,'assignees_url'=>str),(str,'blobs_url'=>str),(str,'branches_url'=>str),(str,'clone_url'=>str),(str,'collaborators_url'=>str),(str,'comments_url'=>str),(str,'commits_url'=>str),(str,'compare_url'=>str),(str,'contents_url'=>str),(str,'contributors_url'=>str),(str,'created_at'=>str),(str,'default_branch'=>str),(str,'deployments_url'=>str),(str,'description'=>Option[str]),(str,'disabled'=>boolean),(str,'downloads_url'=>str),(str,'events_url'=>str),(str,'fork'=>boolean),(str,'forks'=>i64),(str,'forks_count'=>i64),(str,'forks_url'=>str),(str,'full_name'=>str),(str,'git_commits_url'=>str),(str,'git_refs_url'=>str),(str,'git_tags_url'=>str),(str,'git_url'=>str),(str,'has_downloads'=>boolean),(str,'has_issues'=>boolean),(str,'has_pages'=>boolean),(str,'has_projects'=>boolean),(str,'has_wiki'=>boolean),(str,'homepage'=>Option[str]),(str,'hooks_url'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'is_template'=>boolean),(str,'issue_comment_url'=>str),(str,'issue_events_url'=>str),(str,'issues_url'=>str),(str,'keys_url'=>str),(str,'labels_url'=>str),(str,'language'=>Option[str]),(str,'languages_url'=>str),(str,'license'=>Struct[(str,'key'=>str),(str,'name'=>str),(str,'node_id'=>str),(str,'spdx_id'=>str),(str,'url'=>Option[str])]),(str,'master_branch'=>Option[str]),(str,'merges_url'=>str),(str,'milestones_url'=>str),(str,'mirror_url'=>null),(str,'name'=>str),(str,'node_id'=>str),(str,'notifications_url'=>str),(str,'open_issues'=>i64),(str,'open_issues_count'=>i64),(str,'owner'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>boolean),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)]),(str,'private'=>boolean),(str,'public'=>boolean),(str,'pulls_url'=>str),(str,'pushed_at'=>str),(str,'releases_url'=>str),(str,'size'=>i64),(str,'ssh_url'=>str),(str,'stargazers_count'=>i64),(str,'stargazers_url'=>str),(str,'statuses_url'=>str),(str,'subscribers_url'=>str),(str,'subscription_url'=>str),(str,'svn_url'=>str),(str,'tags_url'=>str),(str,'teams_url'=>str),(str,'topics'=>[]),(str,'trees_url'=>str),(str,'updated_at'=>str),(str,'url'=>str),(str,'visibility'=>str),(str,'watchers'=>i64),(str,'watchers_count'=>i64)]),(str,'gist'=>Struct[(str,'comments'=>i64),(str,'created_at'=>str),(str,'description'=>Option[str]),(str,'files'=>{}),(str,'git_pull_url'=>str),(str,'git_push_url'=>str),(str,'html_url'=>str),(str,'id'=>str),(str,'public'=>boolean),(str,'updated_at'=>str),(str,'url'=>str),(str,'user'=>Struct[(str,'avatar_url'=>str),(str,'gravatar_id'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'url'=>str)])]),(str,'head'=>str),(str,'id'=>i64),(str,'issue'=>Struct[(str,'active_lock_reason'=>null),(str,'assignee'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>boolean),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)]),(str,'assignees'=>List[Struct[(str,'avatar_url'->str),(str,'events_url'->str),(str,'followers_url'->str),(str,'following_url'->str),(str,'gists_url'->str),(str,'gravatar_id'->str),(str,'html_url'->str),(str,'id'->i64),(str,'login'->str),(str,'node_id'=>str),(str,'organizations_url'->str),(str,'received_events_url'->str),(str,'repos_url'->str),(str,'site_admin'->boolean),(str,'starred_url'->str),(str,'subscriptions_url'->str),(str,'type'->str),(str,'url'->str)]]),(str,'author_association'=>str),(str,'body'=>Option[str]),(str,'closed_at'=>Option[str]),(str,'comments'=>i64),(str,'comments_url'=>str),(str,'created_at'=>str),(str,'events_url'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'labels'=>List[Struct[(str,'color'->str),(str,'default'=>boolean),(str,'description'=>Option[str]),(str,'id'=>i64),(str,'name'->str),(str,'node_id'=>str),(str,'url'->str)]]),(str,'labels_url'=>str),(str,'locked'=>boolean),(str,'milestone'=>Struct[(str,'closed_at'=>null),(str,'closed_issues'=>i64),(str,'created_at'=>str),(str,'creator'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>boolean),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)]),(str,'description'=>Option[str]),(str,'due_on'=>Option[str]),(str,'html_url'=>str),(str,'id'=>i64),(str,'labels_url'=>str),(str,'node_id'=>str),(str,'number'=>i64),(str,'open_issues'=>i64),(str,'state'=>str),(str,'title'=>str),(str,'updated_at'=>str),(str,'url'=>str)]),(str,'node_id'=>str),(str,'number'=>i64),(str,'performed_via_github_app'=>null),(str,'pull_request'=>Struct[(str,'diff_url'=>Option[str]),(str,'html_url'=>Option[str]),(str,'patch_url'=>Option[str]),(str,'url'=>str)]),(str,'reactions'=>Struct[(str,'url'=>str),(str,'total_count'=>i64),(str,'+1'=>i64),(str,'-1'=>i64),(str,'laugh'=>i64),(str,'hooray'=>i64),(str,'confused'=>i64),(str,'heart'=>i64),(str,'rocket'=>i64),(str,'eyes'=>i64)]),(str,'repository_url'=>str),(str,'state'=>str),(str,'timeline_url'=>str),(str,'title'=>str),(str,'updated_at'=>str),(str,'url'=>str),(str,'user'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>boolean),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)])]),(str,'legacy'=>Struct[(str,'action'=>str),(str,'comment_id'=>i64),(str,'commit'=>str),(str,'desc'=>Option[str]),(str,'head'=>str),(str,'id'=>i64),(str,'issue'=>i64),(str,'issue_id'=>i64),(str,'name'=>str),(str,'number'=>i64),(str,'push_id'=>i64),(str,'ref'=>str),(str,'shas'=>List[List[str]]),(str,'size'=>i64),(str,'url'=>str)]),(str,'master_branch'=>str),(str,'member'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>boolean),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)]),(str,'name'=>str),(str,'number'=>i64),(str,'pages'=>List[Struct[(str,'page_name'->str),(str,'title'->str),(str,'summary'->null),(str,'action'->str),(str,'sha'->str),(str,'html_url'->str)]]),(str,'pull_request'=>Struct[(str,'_links'=>Struct[(str,'self'=>Struct[(str,'href'=>str)]),(str,'html'=>Struct[(str,'href'=>str)]),(str,'issue'=>Struct[(str,'href'=>str)]),(str,'comments'=>Struct[(str,'href'=>str)]),(str,'review_comments'=>Struct[(str,'href'=>str)]),(str,'review_comment'=>Struct[(str,'href'=>str)]),(str,'commits'=>Struct[(str,'href'=>str)]),(str,'statuses'=>Struct[(str,'href'=>str)])]),(str,'active_lock_reason'=>null),(str,'additions'=>i64),(str,'assignee'=>Option[Struct[(str,'avatar_url'->str),(str,'events_url'->str),(str,'followers_url'->str),(str,'following_url'->str),(str,'gists_url'->str),(str,'gravatar_id'->str),(str,'html_url'->str),(str,'id'->i64),(str,'login'->str),(str,'node_id'=>str),(str,'organizations_url'->str),(str,'received_events_url'->str),(str,'repos_url'->str),(str,'site_admin'->boolean),(str,'starred_url'->str),(str,'subscriptions_url'->str),(str,'type'->str),(str,'url'->str)]]),(str,'assignees'=>List[Struct[(str,'avatar_url'->str),(str,'events_url'->str),(str,'followers_url'->str),(str,'following_url'->str),(str,'gists_url'->str),(str,'gravatar_id'->str),(str,'html_url'->str),(str,'id'->i64),(str,'login'->str),(str,'node_id'=>str),(str,'organizations_url'->str),(str,'received_events_url'->str),(str,'repos_url'->str),(str,'site_admin'->boolean),(str,'starred_url'->str),(str,'subscriptions_url'->str),(str,'type'->str),(str,'url'->str)]]),(str,'author_association'=>str),(str,'auto_merge'=>null),(str,'base'=>Struct[(str,'label'=>str),(str,'ref'=>str),(str,'repo'=>Struct[(str,'allow_forking'=>boolean),(str,'archive_url'=>str),(str,'archived'=>boolean),(str,'assignees_url'=>str),(str,'blobs_url'=>str),(str,'branches_url'=>str),(str,'clone_url'=>str),(str,'collaborators_url'=>str),(str,'comments_url'=>str),(str,'commits_url'=>str),(str,'compare_url'=>str),(str,'contents_url'=>str),(str,'contributors_url'=>str),(str,'created_at'=>str),(str,'default_branch'=>str),(str,'deployments_url'=>str),(str,'description'=>Option[str]),(str,'disabled'=>boolean),(str,'downloads_url'=>str),(str,'events_url'=>str),(str,'fork'=>boolean),(str,'forks'=>i64),(str,'forks_count'=>i64),(str,'forks_url'=>str),(str,'full_name'=>str),(str,'git_commits_url'=>str),(str,'git_refs_url'=>str),(str,'git_tags_url'=>str),(str,'git_url'=>str),(str,'has_downloads'=>boolean),(str,'has_issues'=>boolean),(str,'has_pages'=>boolean),(str,'has_projects'=>boolean),(str,'has_wiki'=>boolean),(str,'homepage'=>Option[str]),(str,'hooks_url'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'is_template'=>boolean),(str,'issue_comment_url'=>str),(str,'issue_events_url'=>str),(str,'issues_url'=>str),(str,'keys_url'=>str),(str,'labels_url'=>str),(str,'language'=>Option[str]),(str,'languages_url'=>str),(str,'license'=>Struct[(str,'key'=>str),(str,'name'=>str),(str,'node_id'=>str),(str,'spdx_id'=>str),(str,'url'=>Option[str])]),(str,'master_branch'=>null),(str,'merges_url'=>str),(str,'milestones_url'=>str),(str,'mirror_url'=>Option[str]),(str,'name'=>str),(str,'node_id'=>str),(str,'notifications_url'=>str),(str,'open_issues'=>i64),(str,'open_issues_count'=>i64),(str,'owner'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>boolean),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)]),(str,'private'=>boolean),(str,'pulls_url'=>str),(str,'pushed_at'=>str),(str,'releases_url'=>str),(str,'size'=>i64),(str,'ssh_url'=>str),(str,'stargazers_count'=>i64),(str,'stargazers_url'=>str),(str,'statuses_url'=>str),(str,'subscribers_url'=>str),(str,'subscription_url'=>str),(str,'svn_url'=>str),(str,'tags_url'=>str),(str,'teams_url'=>str),(str,'topics'=>[]),(str,'trees_url'=>str),(str,'updated_at'=>str),(str,'url'=>str),(str,'visibility'=>str),(str,'watchers'=>i64),(str,'watchers_count'=>i64)]),(str,'sha'=>str),(str,'user'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>boolean),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)])]),(str,'body'=>Option[str]),(str,'changed_files'=>i64),(str,'closed_at'=>Option[str]),(str,'comments'=>i64),(str,'comments_url'=>str),(str,'commits'=>i64),(str,'commits_url'=>str),(str,'created_at'=>str),(str,'deletions'=>i64),(str,'diff_url'=>str),(str,'draft'=>boolean),(str,'head'=>Struct[(str,'label'=>str),(str,'ref'=>str),(str,'repo'=>Struct[(str,'allow_forking'=>boolean),(str,'archive_url'=>str),(str,'archived'=>boolean),(str,'assignees_url'=>str),(str,'blobs_url'=>str),(str,'branches_url'=>str),(str,'clone_url'=>str),(str,'collaborators_url'=>str),(str,'comments_url'=>str),(str,'commits_url'=>str),(str,'compare_url'=>str),(str,'contents_url'=>str),(str,'contributors_url'=>str),(str,'created_at'=>str),(str,'default_branch'=>str),(str,'deployments_url'=>str),(str,'description'=>Option[str]),(str,'disabled'=>boolean),(str,'downloads_url'=>str),(str,'events_url'=>str),(str,'fork'=>boolean),(str,'forks'=>i64),(str,'forks_count'=>i64),(str,'forks_url'=>str),(str,'full_name'=>str),(str,'git_commits_url'=>str),(str,'git_refs_url'=>str),(str,'git_tags_url'=>str),(str,'git_url'=>str),(str,'has_downloads'=>boolean),(str,'has_issues'=>boolean),(str,'has_pages'=>boolean),(str,'has_projects'=>boolean),(str,'has_wiki'=>boolean),(str,'homepage'=>Option[str]),(str,'hooks_url'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'is_template'=>boolean),(str,'issue_comment_url'=>str),(str,'issue_events_url'=>str),(str,'issues_url'=>str),(str,'keys_url'=>str),(str,'labels_url'=>str),(str,'language'=>Option[str]),(str,'languages_url'=>str),(str,'license'=>Struct[(str,'key'=>str),(str,'name'=>str),(str,'node_id'=>str),(str,'spdx_id'=>str),(str,'url'=>Option[str])]),(str,'master_branch'=>null),(str,'merges_url'=>str),(str,'milestones_url'=>str),(str,'mirror_url'=>null),(str,'name'=>str),(str,'node_id'=>str),(str,'notifications_url'=>str),(str,'open_issues'=>i64),(str,'open_issues_count'=>i64),(str,'owner'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>boolean),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)]),(str,'private'=>boolean),(str,'pulls_url'=>str),(str,'pushed_at'=>str),(str,'releases_url'=>str),(str,'size'=>i64),(str,'ssh_url'=>str),(str,'stargazers_count'=>i64),(str,'stargazers_url'=>str),(str,'statuses_url'=>str),(str,'subscribers_url'=>str),(str,'subscription_url'=>str),(str,'svn_url'=>str),(str,'tags_url'=>str),(str,'teams_url'=>str),(str,'topics'=>[]),(str,'trees_url'=>str),(str,'updated_at'=>str),(str,'url'=>str),(str,'visibility'=>str),(str,'watchers'=>i64),(str,'watchers_count'=>i64)]),(str,'sha'=>str),(str,'user'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>boolean),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)])]),(str,'html_url'=>str),(str,'id'=>i64),(str,'issue_url'=>str),(str,'labels'=>List[Struct[(str,'color'->str),(str,'default'->boolean),(str,'description'=>null),(str,'id'->i64),(str,'name'->str),(str,'node_id'->str),(str,'url'->str)]]),(str,'locked'=>boolean),(str,'maintainer_can_modify'=>boolean),(str,'merge_commit_sha'=>Option[str]),(str,'mergeable'=>Option[boolean]),(str,'mergeable_state'=>str),(str,'merged'=>boolean),(str,'merged_at'=>Option[str]),(str,'merged_by'=>Option[Struct[(str,'avatar_url'->str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'->str),(str,'html_url'=>str),(str,'id'->i64),(str,'login'->str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>boolean),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'->str)]]),(str,'milestone'=>Option[Struct[(str,'closed_at'->null),(str,'closed_issues'->i64),(str,'created_at'->str),(str,'creator'->Struct[(str,'avatar_url'->str),(str,'events_url'->str),(str,'followers_url'->str),(str,'following_url'->str),(str,'gists_url'->str),(str,'gravatar_id'->str),(str,'html_url'->str),(str,'id'->i64),(str,'login'->str),(str,'node_id'=>str),(str,'organizations_url'->str),(str,'received_events_url'->str),(str,'repos_url'->str),(str,'site_admin'->boolean),(str,'starred_url'->str),(str,'subscriptions_url'->str),(str,'type'->str),(str,'url'->str)]),(str,'description'->str),(str,'due_on'->Option[str]),(str,'html_url'->str),(str,'id'->i64),(str,'labels_url'->str),(str,'node_id'=>str),(str,'number'->i64),(str,'open_issues'->i64),(str,'state'->str),(str,'title'->str),(str,'updated_at'->str),(str,'url'->str)]]),(str,'node_id'=>str),(str,'number'=>i64),(str,'patch_url'=>str),(str,'rebaseable'=>Option[boolean]),(str,'requested_reviewers'=>List[Struct[(str,'login'->str),(str,'id'->i64),(str,'node_id'->str),(str,'avatar_url'->str),(str,'gravatar_id'->str),(str,'url'->str),(str,'html_url'->str),(str,'followers_url'->str),(str,'following_url'->str),(str,'gists_url'->str),(str,'starred_url'->str),(str,'subscriptions_url'->str),(str,'organizations_url'->str),(str,'repos_url'->str),(str,'events_url'->str),(str,'received_events_url'->str),(str,'type'->str),(str,'site_admin'->boolean)]]),(str,'requested_teams'=>[]),(str,'review_comment_url'=>str),(str,'review_comments'=>i64),(str,'review_comments_url'=>str),(str,'state'=>str),(str,'statuses_url'=>str),(str,'title'=>str),(str,'updated_at'=>str),(str,'url'=>str),(str,'user'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>boolean),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)])]),(str,'push_id'=>i64),(str,'pusher_type'=>str),(str,'ref'=>Option[str]),(str,'ref_type'=>str),(str,'release'=>Struct[(str,'assets'=>List[Struct[(str,'browser_download_url'->str),(str,'content_type'->str),(str,'created_at'->str),(str,'download_count'->i64),(str,'id'->i64),(str,'label'->Option[str]),(str,'name'->str),(str,'size'->i64),(str,'state'->str),(str,'updated_at'->str),(str,'uploader'->Struct[(str,'login'->str),(str,'id'->i64),(str,'avatar_url'->str),(str,'gravatar_id'->str),(str,'url'->str),(str,'html_url'->str),(str,'followers_url'->str),(str,'following_url'->str),(str,'gists_url'->str),(str,'starred_url'->str),(str,'subscriptions_url'->str),(str,'organizations_url'->str),(str,'repos_url'->str),(str,'events_url'->str),(str,'received_events_url'->str),(str,'type'->str),(str,'site_admin'->boolean)]),(str,'url'->str)]]),(str,'assets_url'=>str),(str,'author'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>boolean),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)]),(str,'body'=>Option[str]),(str,'created_at'=>str),(str,'draft'=>boolean),(str,'html_url'=>str),(str,'id'=>i64),(str,'is_short_description_html_truncated'=>boolean),(str,'name'=>Option[str]),(str,'node_id'=>str),(str,'prerelease'=>boolean),(str,'published_at'=>str),(str,'short_description_html'=>str),(str,'tag_name'=>str),(str,'tarball_url'=>str),(str,'target_commitish'=>str),(str,'upload_url'=>str),(str,'url'=>str),(str,'zipball_url'=>str)]),(str,'review'=>Struct[(str,'_links'=>Struct[(str,'html'=>Struct[(str,'href'=>str)]),(str,'pull_request'=>Struct[(str,'href'=>str)])]),(str,'author_association'=>str),(str,'body'=>Option[str]),(str,'commit_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'node_id'=>str),(str,'pull_request_url'=>str),(str,'state'=>str),(str,'submitted_at'=>str),(str,'user'=>Struct[(str,'login'=>str),(str,'id'=>i64),(str,'node_id'=>str),(str,'avatar_url'=>str),(str,'gravatar_id'=>str),(str,'url'=>str),(str,'html_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'organizations_url'=>str),(str,'repos_url'=>str),(str,'events_url'=>str),(str,'received_events_url'=>str),(str,'type'=>str),(str,'site_admin'=>boolean)])]),(str,'size'=>i64),(str,'url'=>str)]],Option[str],Option[Struct[(str,'id'=>i64),(str,'name'=>str),(str,'url'=>str)]],Option[Struct[(str,'avatar_url'=>str),(str,'gravatar_id'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'url'=>str)]],null,null,Option[str])";
    auto type = python::Type::decode(type_str);
    EXPECT_NE(type, python::Type::UNKNOWN);

    // take now sample row from github_daily and match using helper function...
    string path = "../resources/hyperspecialization/github_daily/2016-10-15.json.sample";
    auto data = fileToString(path);
    auto lines = splitToLines(data);
    ASSERT_FALSE(lines.empty());
    string line = lines.front();

    auto columns = std::vector<std::string>({"type", "public", "actor", "created_at",
                                             "payload", "id", "repo", "org", "repository", "actor_attributes", "url"});

    // explain match
    explainJsonMatch(line, type, columns, true);
}

TEST_F(JsonTuplexTest, BasicLoad) {
    using namespace tuplex;
    using namespace std;

    auto opt = microTestOptions();
    opt.set("tuplex.executorCount", "0"); // start single-threaded
    Context ctx(opt);
    bool unwrap_first_level = true;

    // this seems to work.
    // should show:
    // 10, None, 3.414
    // 2, 42, 12.0      <-- note the order correction
    // None, None, 2    <-- note the order (fallback/general case)
    unwrap_first_level = true;
     ctx.json("../resources/ndjson/example2.json", unwrap_first_level).show();
    auto v = ctx.json("../resources/ndjson/example2.json", unwrap_first_level).collectAsVector();
    ASSERT_EQ(v.size(), 3);
    EXPECT_EQ(v[0].toPythonString(), "(10,None,3.41400)");
    EXPECT_EQ(v[1].toPythonString(), "(2,42,12.00000)");
    EXPECT_EQ(v[2].toPythonString(), "(None,None,2.00000)");


//    unwrap_first_level = false; // --> requires implementing/adding decoding of struct dict...
//    ctx.json("../resources/ndjson/example2.json", unwrap_first_level).show();
//    // // large json file (to make sure buffers etc. work
//    // ctx.json("/data/2014-10-15.json").show();
}

TEST_F(JsonTuplexTest, BasicLoadWithSlowDecode) {
    using namespace tuplex;
    using namespace std;

    auto opt = microTestOptions();
    opt.set("tuplex.executorCount", "0"); // start single-threaded
    Context ctx(opt);
    bool unwrap_first_level = false; // --> requires implementing/adding decoding of struct dict...
    ctx.json("../resources/ndjson/example2.json", unwrap_first_level).show();
}

// dev func here
namespace tuplex {
    Field json_string_to_field(const std::string& s, const python::Type& type) {
        // base cases:
        if(type == python::Type::NULLVALUE) {
            return Field::null();
        } else if(type == python::Type::BOOLEAN) {
            return Field(stringToBool(s));
        } else if(type == python::Type::I64) {
            return Field(parseI64String(s));
        } else if(type == python::Type::F64) {
            return Field(parseF64String(s));
        } else if(type.isOptionType()) {
            if(s == "null")
                return Field::null();
            else {
                return json_string_to_field(s, type.getReturnType());
            }
        } else {
            throw std::runtime_error("Unknown type " + type.desc() + " encountered.");
        }
    }
}


TEST_F(JsonTuplexTest, JsonToRow) {
    using namespace tuplex;
    using namespace std;
    // dev for decoding struct dicts (unfortunately necessary...)
    // need memory -> Row

    // and Row -> python

    auto content = "{\"repo\":{\"id\":355634,\"url\":\"https://api.github.dev/repos/projectblacklight/blacklight\",\"name\":\"projectblacklight/blacklight\"},\"type\":\"PushEvent\",\"org\":{\"gravatar_id\":\"6cb76a4a521c36d96a0583e7c45eaf95\",\"id\":120516,\"url\":\"https://api.github.dev/orgs/projectblacklight\",\"avatar_url\":\"https://secure.gravatar.com/avatar/6cb76a4a521c36d96a0583e7c45eaf95?d=http://github.dev%2Fimages%2Fgravatars%2Fgravatar-org-420.png\",\"login\":\"projectblacklight\"},\"public\":true,\"created_at\":\"2011-02-12T00:00:00Z\",\"payload\":{\"shas\":[[\"d3da39ab96a2caecae5d526596a04820c6f848a6\",\"b31a437f57bdf70558bed8ac28790a53e8174b87@stanford.edu\",\"We have to stay with cucumber < 0.10 and rspec < 2, otherwise our tests break. I updated the gem requirements to reflect this, so people won't accidentally upgrade to a gem version that's too new when they run rake gems:install\",\"Bess Sadler\"],[\"898150b7830102cdc171cbd4304b6a783101c3e3\",\"b31a437f57bdf70558bed8ac28790a53e8174b87@stanford.edu\",\"Removing unused cruise control tasks. Also, we shouldn't remove the coverage.data file, so we can look at the coverage data over time.\",\"Bess Sadler\"]],\"repo\":\"projectblacklight/blacklight\",\"actor\":\"bess\",\"ref\":\"refs/heads/master\",\"size\":2,\"head\":\"898150b7830102cdc171cbd4304b6a783101c3e3\",\"actor_gravatar\":\"887fa2fcd0cf1cbdc6dc43e5524f33f6\",\"push_id\":24643024},\"actor\":{\"gravatar_id\":\"887fa2fcd0cf1cbdc6dc43e5524f33f6\",\"id\":65608,\"url\":\"https://api.github.dev/users/bess\",\"avatar_url\":\"https://secure.gravatar.com/avatar/887fa2fcd0cf1cbdc6dc43e5524f33f6?d=http://github.dev%2Fimages%2Fgravatars%2Fgravatar-user-420.png\",\"login\":\"bess\"},\"id\":\"1127195475\"}";

    // parse as row, and then convert to python!
    auto rows = parseRowsFromJSON(content, nullptr, false);

    ASSERT_EQ(rows.size(), 1);
    auto row = rows.front();

    python::lockGIL();
    try {
        auto obj = json_string_to_pyobject(row.getString(0), row.getType(0));
        PyObject_Print(obj, stdout, 0); std::cout<<std::endl;
    } catch(...) {
        std::cerr<<"exception occurred"<<std::endl;
    }
    python::unlockGIL();

    // let's start with JSON string -> Row.
    //auto f = json_string_to_field("{")
}

namespace tuplex {
    namespace codegen {
        // helper function to generate a quick json-buffer parse and match against general-case check
        std::function<int64_t(const uint8_t*, size_t, uint8_t**, size_t*)> generateJsonTestParse(JITCompiler& jit, const python::Type& dict_type) {
            using namespace llvm;
            using namespace std;

            assert(dict_type.isStructuredDictionaryType());

            std::string func_name = "json_test_parse";

            LLVMEnvironment env;

            // create func
            auto& ctx = env.getContext();
            auto func = getOrInsertFunction(env.getModule().get(), func_name, ctypeToLLVM<int64_t>(ctx), ctypeToLLVM<uint8_t*>(ctx),
                    ctypeToLLVM<int64_t>(ctx),
                            ctypeToLLVM<uint8_t**>(ctx),
                                    ctypeToLLVM<int64_t*>(ctx));

            auto argMap = mapLLVMFunctionArgs(func, {"buf", "buf_size", "out", "out_size"});

            BasicBlock* bBody = BasicBlock::Create(ctx, "entry", func);
            IRBuilder<> builder(bBody);

            // create mismatch block
            BasicBlock* bbMismatch = BasicBlock::Create(ctx, "mismatch", func);

            // init parser
            auto Finitparser = getOrInsertFunction(env.getModule().get(), "JsonParser_Init", env.i8ptrType());

            auto parser = builder.CreateCall(Finitparser, {});

            auto F = getOrInsertFunction(env.getModule().get(), "JsonParser_open", env.i64Type(), env.i8ptrType(),
                                         env.i8ptrType(), env.i64Type());
            auto buf = argMap["buf"];
            auto buf_size = argMap["buf_size"];
            auto open_rc = builder.CreateCall(F, {parser, buf, buf_size});

            // get initial object
            // => this is from parser
            auto Fgetobj = getOrInsertFunction(env.getModule().get(), "JsonParser_getObject", env.i64Type(),
                                               env.i8ptrType(), env.i8ptrType()->getPointerTo(0));

            auto row_object_var = env.CreateFirstBlockVariable(builder, env.i8nullptr(), "row_object");
            auto obj_var = row_object_var;

            builder.CreateCall(Fgetobj, {parser, obj_var});

            // don't forget to free everything...
            // alloc variable
            auto struct_dict_type = create_structured_dict_type(env, dict_type);
            auto row_var = env.CreateFirstBlockAlloca(builder, struct_dict_type);
            struct_dict_mem_zero(env, builder, row_var, dict_type); // !!! important !!!

            // parse now
            JSONParseRowGenerator gen(env, dict_type, bbMismatch);
            gen.parseToVariable(builder, builder.CreateLoad(obj_var), row_var);

            // ok, now serialize
            auto s_length = struct_dict_serialized_memory_size(env, builder, row_var, dict_type).val;
            env.printValue(builder, s_length, "serialized size is: ");
            auto out_buf = env.cmalloc(builder, s_length);

            // serialize
            struct_dict_serialize_to_memory(env, builder, row_var, dict_type, out_buf);

            builder.CreateStore(s_length, argMap["out_size"]);
            builder.CreateStore(out_buf, argMap["out"]);

            // do not care about leaks in this function...
            builder.CreateRet(env.i64Const(0));

            builder.SetInsertPoint(bbMismatch);
            builder.CreateRet(env.i64Const(ecToI64(ExceptionCode::BADPARSE_STRING_INPUT)));

            // --- gen done ---, compile...
            bool rc_compile = jit.compile(std::move(env.getModule()));

            if(!rc_compile)
                throw std::runtime_error("compile error for module");

            auto ptr = jit.getAddrOfSymbol(func_name);
            return reinterpret_cast<int64_t(*)(const uint8_t*, size_t, uint8_t**, size_t*)>(ptr);
        }

        // helper function to generate a quick json-buffer parse and match against general-case check
        std::function<int64_t(const uint8_t*, size_t, uint8_t**, size_t*)> generateJsonTupleTestParse(JITCompiler& jit,
                                                                                                      const python::Type& tuple_type,
                                                                                                      const std::vector<std::string>& columns) {
            using namespace llvm;
            using namespace std;

            assert(tuple_type.isTupleType());
            assert(columns.size() == tuple_type.parameters().size());

            std::string func_name = "json_test_tuple_parse";

            LLVMEnvironment env;

            // create func
            auto& ctx = env.getContext();
            auto func = getOrInsertFunction(env.getModule().get(), func_name, ctypeToLLVM<int64_t>(ctx), ctypeToLLVM<uint8_t*>(ctx),
                                            ctypeToLLVM<int64_t>(ctx),
                                            ctypeToLLVM<uint8_t**>(ctx),
                                            ctypeToLLVM<int64_t*>(ctx));

            auto argMap = mapLLVMFunctionArgs(func, {"buf", "buf_size", "out", "out_size"});

            BasicBlock* bBody = BasicBlock::Create(ctx, "entry", func);
            IRBuilder<> builder(bBody);

            // create mismatch block
            BasicBlock* bbMismatch = BasicBlock::Create(ctx, "mismatch", func);

            // init parser
            auto Finitparser = getOrInsertFunction(env.getModule().get(), "JsonParser_Init", env.i8ptrType());

            auto parser = builder.CreateCall(Finitparser, {});

            auto F = getOrInsertFunction(env.getModule().get(), "JsonParser_open", env.i64Type(), env.i8ptrType(),
                                         env.i8ptrType(), env.i64Type());
            auto buf = argMap["buf"];
            auto buf_size = argMap["buf_size"];
            auto open_rc = builder.CreateCall(F, {parser, buf, buf_size});

            // get initial object
            // => this is from parser
            auto Fgetobj = getOrInsertFunction(env.getModule().get(), "JsonParser_getObject", env.i64Type(),
                                               env.i8ptrType(), env.i8ptrType()->getPointerTo(0));

            auto row_object_var = env.CreateFirstBlockVariable(builder, env.i8nullptr(), "row_object");
            auto obj_var = row_object_var;

            builder.CreateCall(Fgetobj, {parser, obj_var});

            // create dict type from columns & tuple
            std::vector<python::StructEntry> pairs;
            for(unsigned i = 0; i < tuple_type.parameters().size(); ++i) {
                python::StructEntry e;
                e.key = escape_to_python_str(columns[i]);
                e.keyType = python::Type::STRING;
                e.valueType = tuple_type.parameters()[i];
                pairs.push_back(e);
            }
            auto dict_type = python::Type::makeStructuredDictType(pairs);

            // don't forget to free everything...
            // alloc variable
            auto struct_dict_type = create_structured_dict_type(env, dict_type);
            auto row_var = env.CreateFirstBlockAlloca(builder, struct_dict_type);
            struct_dict_mem_zero(env, builder, row_var, dict_type); // !!! important !!!

            // parse now
            JSONParseRowGenerator gen(env, dict_type, bbMismatch);
            gen.parseToVariable(builder, builder.CreateLoad(obj_var), row_var);

            // convert to tuple now and then serialize tuple
            // fetch columns from dict and assign to tuple!
            FlattenedTuple ft(&env);
            ft.init(tuple_type);
            auto num_entries = tuple_type.parameters().size();
            for(int i = 0; i < num_entries; ++i) {
                SerializableValue value;

                // fetch value from dict!
                value = struct_dict_get_or_except(env, builder, dict_type, escape_to_python_str(columns[i]),
                                                  python::Type::STRING, row_var, bbMismatch);

                ft.set(builder, {i}, value.val, value.size, value.is_null);
            }


            // ok, now serialize
            auto s_length = ft.getSize(builder);
            env.printValue(builder, s_length, "serialized size is: ");
            auto out_buf = env.cmalloc(builder, s_length);

            // serialize
            ft.serialize(builder, out_buf);

            builder.CreateStore(s_length, argMap["out_size"]);
            builder.CreateStore(out_buf, argMap["out"]);

            // do not care about leaks in this function...
            builder.CreateRet(env.i64Const(0));

            builder.SetInsertPoint(bbMismatch);
            builder.CreateRet(env.i64Const(ecToI64(ExceptionCode::BADPARSE_STRING_INPUT)));


            // optimize
            LLVMOptimizer opt; opt.optimizeModule(*env.getModule());

            //annotateModuleWithInstructionPrint(*env.getModule());

            // --- gen done ---, compile...
            bool rc_compile = jit.compile(std::move(env.getModule()));

            if(!rc_compile)
                throw std::runtime_error("compile error for module");

            auto ptr = jit.getAddrOfSymbol(func_name);
            return reinterpret_cast<int64_t(*)(const uint8_t*, size_t, uint8_t**, size_t*)>(ptr);
        }
    }
}

TEST_F(JsonTuplexTest, TupleBinToPythonSimple) {
//    using namespace tuplex;
//    using namespace std;
//
////    string test_type_str = "(str,Struct[(str,'shas'=>List[List[str]])])";
//    string test_type_str = "(str,Struct[(str,'shas'=>List[(str,str,i64)])])";
//    auto test_type = python::Type::decode(test_type_str);
//    ASSERT_TRUE(test_type.isTupleType());
//    EXPECT_EQ(test_type.parameters().size(), 2);
//    std::cout<<"testing with tuple type " + test_type.desc() + "..."<<std::endl;
//
//    // sample line.
////    string line = "{\"A\":\"hello world!\",\"col\": {\"shas\":[[\"a\",\"b\"],[],[\"c\"]]}}";
//    string line = "{\"A\":\"hello world!\",\"col\": {\"shas\":[[\"a\",\"b\",42],[\"c\",\"d\",12]]}}";
//
//    runtime::init(ContextOptions::defaults().RUNTIME_LIBRARY().toPath());
//
//    JITCompiler jit;
//
//    // create parse function (easier to debug!)
//    std::vector<std::string> columns({"A", "col"});
//    auto f = codegen::generateJsonTupleTestParse(jit, test_type, columns);
//    ASSERT_TRUE(f);
//
//    unsigned line_no = 0;
//
//    uint8_t *buf = nullptr;
//    size_t size = 0;
//    auto rc = f(reinterpret_cast<const uint8_t *>(line.c_str()), line.size(), &buf, &size);
//    EXPECT_EQ(rc, 0);

    using namespace tuplex;
    using namespace std;

    auto general_type_str = "(str,Struct[(str,'action'=>str),(str,'comment'=>Struct[(str,'url'->str),(str,'id'->i64),(str,'diff_hunk'->str),(str,'path'->str),(str,'position'->i64),(str,'original_position'->i64),(str,'commit_id'->str),(str,'original_commit_id'->str),(str,'user'->Struct[(str,'login'->str),(str,'id'->i64),(str,'avatar_url'->str),(str,'gravatar_id'->str),(str,'url'->str),(str,'html_url'->str),(str,'followers_url'->str),(str,'following_url'->str),(str,'gists_url'->str),(str,'starred_url'->str),(str,'subscriptions_url'->str),(str,'organizations_url'->str),(str,'repos_url'->str),(str,'events_url'->str),(str,'received_events_url'->str),(str,'type'->str),(str,'site_admin'->boolean)]),(str,'body'->str),(str,'created_at'->str),(str,'updated_at'->str),(str,'html_url'->str),(str,'pull_request_url'->str),(str,'_links'->Struct[(str,'self'->Struct[(str,'href'->str)]),(str,'html'->Struct[(str,'href'->str)]),(str,'pull_request'->Struct[(str,'href'->str)])])]),(str,'comment_id'=>i64),(str,'commit'=>str),(str,'desc'=>str),(str,'description'=>str),(str,'head'=>str),(str,'id'=>i64),(str,'issue'=>i64),(str,'issue_id'=>i64),(str,'master_branch'=>str),(str,'member'=>Struct[(str,'login'->str),(str,'id'->i64),(str,'avatar_url'->str),(str,'gravatar_id'->str),(str,'url'->str),(str,'html_url'->str),(str,'followers_url'->str),(str,'following_url'->str),(str,'gists_url'->str),(str,'starred_url'->str),(str,'subscriptions_url'->str),(str,'organizations_url'->str),(str,'repos_url'->str),(str,'events_url'->str),(str,'received_events_url'->str),(str,'type'->str),(str,'site_admin'->boolean)]),(str,'name'=>str),(str,'number'=>i64),(str,'pages'=>List[Struct[(str,'page_name'->str),(str,'title'->str),(str,'summary'->null),(str,'action'->str),(str,'sha'->str),(str,'html_url'->str)]]),(str,'pull_request'=>Struct[(str,'_links'->Struct[(str,'self'->Struct[(str,'href'->str)]),(str,'html'->Struct[(str,'href'->str)]),(str,'issue'->Struct[(str,'href'->str)]),(str,'comments'->Struct[(str,'href'->str)]),(str,'review_comments'->Struct[(str,'href'->str)]),(str,'statuses'->Struct[(str,'href'->str)])]),(str,'additions'->i64),(str,'assignee'->null),(str,'base'->Struct[(str,'label'->str),(str,'ref'->str),(str,'repo'->Struct[(str,'archive_url'->str),(str,'assignees_url'->str),(str,'blobs_url'->str),(str,'branches_url'->str),(str,'clone_url'->str),(str,'collaborators_url'->str),(str,'comments_url'->str),(str,'commits_url'->str),(str,'compare_url'->str),(str,'contents_url'->str),(str,'contributors_url'->str),(str,'created_at'->str),(str,'default_branch'->str),(str,'description'->str),(str,'downloads_url'->str),(str,'events_url'->str),(str,'fork'->boolean),(str,'forks'->i64),(str,'forks_count'->i64),(str,'forks_url'->str),(str,'full_name'->str),(str,'git_commits_url'->str),(str,'git_refs_url'->str),(str,'git_tags_url'->str),(str,'git_url'->str),(str,'has_downloads'->boolean),(str,'has_issues'->boolean),(str,'has_wiki'->boolean),(str,'homepage'->Option[str]),(str,'hooks_url'->str),(str,'html_url'->str),(str,'id'->i64),(str,'issue_comment_url'->str),(str,'issue_events_url'->str),(str,'issues_url'->str),(str,'keys_url'->str),(str,'labels_url'->str),(str,'language'->str),(str,'languages_url'->str),(str,'master_branch'->str),(str,'merges_url'->str),(str,'milestones_url'->str),(str,'mirror_url'->null),(str,'name'->str),(str,'notifications_url'->str),(str,'open_issues'->i64),(str,'open_issues_count'->i64),(str,'owner'->Struct[(str,'avatar_url'->str),(str,'events_url'->str),(str,'followers_url'->str),(str,'following_url'->str),(str,'gists_url'->str),(str,'gravatar_id'->Option[str]),(str,'html_url'->str),(str,'id'->i64),(str,'login'->str),(str,'organizations_url'->str),(str,'received_events_url'->str),(str,'repos_url'->str),(str,'site_admin'->boolean),(str,'starred_url'->str),(str,'subscriptions_url'->str),(str,'type'->str),(str,'url'->str)]),(str,'private'->boolean),(str,'pulls_url'->str),(str,'pushed_at'->str),(str,'size'->i64),(str,'ssh_url'->str),(str,'stargazers_url'->str),(str,'statuses_url'->str),(str,'subscribers_url'->str),(str,'subscription_url'->str),(str,'svn_url'->str),(str,'tags_url'->str),(str,'teams_url'->str),(str,'trees_url'->str),(str,'updated_at'->str),(str,'url'->str),(str,'watchers'->i64),(str,'watchers_count'->i64)]),(str,'sha'->str),(str,'user'->Struct[(str,'avatar_url'->str),(str,'events_url'->str),(str,'followers_url'->str),(str,'following_url'->str),(str,'gists_url'->str),(str,'gravatar_id'->Option[str]),(str,'html_url'->str),(str,'id'->i64),(str,'login'->str),(str,'organizations_url'->str),(str,'received_events_url'->str),(str,'repos_url'->str),(str,'site_admin'->boolean),(str,'starred_url'->str),(str,'subscriptions_url'->str),(str,'type'->str),(str,'url'->str)])]),(str,'body'->str),(str,'changed_files'->i64),(str,'closed_at'->Option[str]),(str,'comments'->i64),(str,'comments_url'->str),(str,'commits'->i64),(str,'commits_url'->str),(str,'created_at'->str),(str,'deletions'->i64),(str,'diff_url'->str),(str,'head'->Struct[(str,'label'->str),(str,'ref'->str),(str,'repo'->Struct[(str,'archive_url'->str),(str,'assignees_url'->str),(str,'blobs_url'->str),(str,'branches_url'->str),(str,'clone_url'->str),(str,'collaborators_url'->str),(str,'comments_url'->str),(str,'commits_url'->str),(str,'compare_url'->str),(str,'contents_url'->str),(str,'contributors_url'->str),(str,'created_at'->str),(str,'default_branch'->str),(str,'description'->str),(str,'downloads_url'->str),(str,'events_url'->str),(str,'fork'->boolean),(str,'forks'->i64),(str,'forks_count'->i64),(str,'forks_url'->str),(str,'full_name'->str),(str,'git_commits_url'->str),(str,'git_refs_url'->str),(str,'git_tags_url'->str),(str,'git_url'->str),(str,'has_downloads'->boolean),(str,'has_issues'->boolean),(str,'has_wiki'->boolean),(str,'homepage'->Option[str]),(str,'hooks_url'->str),(str,'html_url'->str),(str,'id'->i64),(str,'issue_comment_url'->str),(str,'issue_events_url'->str),(str,'issues_url'->str),(str,'keys_url'->str),(str,'labels_url'->str),(str,'language'->str),(str,'languages_url'->str),(str,'master_branch'->str),(str,'merges_url'->str),(str,'milestones_url'->str),(str,'mirror_url'->null),(str,'name'->str),(str,'notifications_url'->str),(str,'open_issues'->i64),(str,'open_issues_count'->i64),(str,'owner'->Struct[(str,'avatar_url'->str),(str,'events_url'->str),(str,'followers_url'->str),(str,'following_url'->str),(str,'gists_url'->str),(str,'gravatar_id'->Option[str]),(str,'html_url'->str),(str,'id'->i64),(str,'login'->str),(str,'organizations_url'->str),(str,'received_events_url'->str),(str,'repos_url'->str),(str,'site_admin'->boolean),(str,'starred_url'->str),(str,'subscriptions_url'->str),(str,'type'->str),(str,'url'->str)]),(str,'private'->boolean),(str,'pulls_url'->str),(str,'pushed_at'->str),(str,'size'->i64),(str,'ssh_url'->str),(str,'stargazers_url'->str),(str,'statuses_url'->str),(str,'subscribers_url'->str),(str,'subscription_url'->str),(str,'svn_url'->str),(str,'tags_url'->str),(str,'teams_url'->str),(str,'trees_url'->str),(str,'updated_at'->str),(str,'url'->str),(str,'watchers'->i64),(str,'watchers_count'->i64)]),(str,'sha'->str),(str,'user'->Struct[(str,'avatar_url'->str),(str,'events_url'->str),(str,'followers_url'->str),(str,'following_url'->str),(str,'gists_url'->str),(str,'gravatar_id'->Option[str]),(str,'html_url'->str),(str,'id'->i64),(str,'login'->str),(str,'organizations_url'->str),(str,'received_events_url'->str),(str,'repos_url'->str),(str,'site_admin'->boolean),(str,'starred_url'->str),(str,'subscriptions_url'->str),(str,'type'->str),(str,'url'->str)])]),(str,'html_url'->str),(str,'id'->i64),(str,'issue_url'->str),(str,'merge_commit_sha'->Option[str]),(str,'mergeable'->Option[boolean]),(str,'mergeable_state'->str),(str,'merged'->boolean),(str,'merged_at'->Option[str]),(str,'merged_by'->Option[Struct[(str,'login'->str),(str,'id'->i64),(str,'avatar_url'->str),(str,'gravatar_id'->str),(str,'url'->str),(str,'html_url'->str),(str,'followers_url'->str),(str,'following_url'->str),(str,'gists_url'->str),(str,'starred_url'->str),(str,'subscriptions_url'->str),(str,'organizations_url'->str),(str,'repos_url'->str),(str,'events_url'->str),(str,'received_events_url'->str),(str,'type'->str),(str,'site_admin'->boolean)]]),(str,'milestone'->null),(str,'number'->i64),(str,'patch_url'->str),(str,'review_comment_url'->str),(str,'review_comments'->i64),(str,'review_comments_url'->str),(str,'state'->str),(str,'statuses_url'->str),(str,'title'->str),(str,'updated_at'->str),(str,'url'->str),(str,'user'->Struct[(str,'login'->str),(str,'id'->i64),(str,'avatar_url'->str),(str,'gravatar_id'->str),(str,'url'->str),(str,'html_url'->str),(str,'followers_url'->str),(str,'following_url'->str),(str,'gists_url'->str),(str,'starred_url'->str),(str,'subscriptions_url'->str),(str,'organizations_url'->str),(str,'repos_url'->str),(str,'events_url'->str),(str,'received_events_url'->str),(str,'type'->str),(str,'site_admin'->boolean)])]),(str,'ref'=>Option[str]),(str,'ref_type'=>str),(str,'shas'=>List[(str,str,str,str,boolean)]),(str,'size'=>i64),(str,'target'=>Struct[(str,'id'->i64),(str,'login'->str),(str,'followers'->i64),(str,'repos'->i64),(str,'gravatar_id'->str)]),(str,'url'=>str)],boolean,str,str,Option[str],Struct[(str,'blog'=>str),(str,'company'=>str),(str,'email'->str),(str,'gravatar_id'->str),(str,'location'=>str),(str,'login'->str),(str,'name'=>str),(str,'type'->str)],Struct[(str,'created_at'->str),(str,'description'=>str),(str,'fork'->boolean),(str,'forks'->i64),(str,'has_downloads'->boolean),(str,'has_issues'->boolean),(str,'has_wiki'->boolean),(str,'homepage'=>str),(str,'id'->i64),(str,'integrate_branch'=>str),(str,'language'=>str),(str,'master_branch'->str),(str,'name'->str),(str,'open_issues'->i64),(str,'organization'=>str),(str,'owner'->str),(str,'private'->boolean),(str,'pushed_at'->str),(str,'size'->i64),(str,'stargazers'->i64),(str,'url'->str),(str,'watchers'->i64)])";
    auto bad_row = "{\"created_at\":\"2013-10-15T00:07:50-07:00\",\"payload\":{\"shas\":[[\"18081d7ca4f4870dc88af0ff2d34027c8ac4200f\",\"5395ebfd174b0a5617e6f409dfbb3e064e3fdf0a@.(none)\",\"inistial\",\"unknown\",true],[\"ba4ba8296f0ebe79b999c788c54ad4573c9f96a6\",\"5395ebfd174b0a5617e6f409dfbb3e064e3fdf0a@.(none)\",\"modify the index.html\",\"unknown\",true],[\"7d12c2ff93149dff5eee8e68c9a66a1679531784\",\"5395ebfd174b0a5617e6f409dfbb3e064e3fdf0a@.(none)\",\"Merge branch 'master' of https://github.com/idpocky/anjularjs-demo\\n\\nConflicts:\\n\\tREADME.md\",\"unknown\",true]],\"size\":3,\"ref\":\"refs/heads/master\",\"head\":\"7d12c2ff93149dff5eee8e68c9a66a1679531784\"},\"public\":true,\"type\":\"PushEvent\",\"url\":\"https://github.com/idpocky/anjularjs-demo/compare/476e2955cd...7d12c2ff93\",\"actor\":\"idpocky\",\"actor_attributes\":{\"login\":\"idpocky\",\"type\":\"User\",\"gravatar_id\":\"a4550370e4f2af2be8a3907b92e2a912\",\"email\":\"da39a3ee5e6b4b0d3255bfef95601890afd80709\"},\"repository\":{\"id\":13582723,\"name\":\"anjularjs-demo\",\"url\":\"https://github.com/idpocky/anjularjs-demo\",\"description\":\"a demo of anjularjs\",\"watchers\":0,\"stargazers\":0,\"forks\":0,\"fork\":false,\"size\":0,\"owner\":\"idpocky\",\"private\":false,\"open_issues\":0,\"has_issues\":true,\"has_downloads\":true,\"has_wiki\":true,\"created_at\":\"2013-10-14T23:55:30-07:00\",\"pushed_at\":\"2013-10-15T00:07:50-07:00\",\"master_branch\":\"master\"}}";

    auto test_type = python::Type::decode(general_type_str);
    ASSERT_TRUE(test_type.isTupleType());
    EXPECT_EQ(test_type.parameters().size(), 8);
    std::cout<<"testing with tuple type " + test_type.desc() + "..."<<std::endl;

    // sample line.
    string line = bad_row;

    runtime::init(ContextOptions::defaults().RUNTIME_LIBRARY().toPath());

    JITCompiler jit;

    // create parse function (easier to debug!)
    std::vector<std::string> columns({"created_at", "payload", "public", "type", "url", "actor", "actor_attributes", "repository"});
    auto f = codegen::generateJsonTupleTestParse(jit, test_type, columns);
    ASSERT_TRUE(f);

    unsigned line_no = 0;

    uint8_t *buf = nullptr;
    size_t size = 0;
    auto rc = f(reinterpret_cast<const uint8_t *>(line.c_str()), line.size(), &buf, &size);
    EXPECT_EQ(rc, 0);

}

TEST_F(JsonTuplexTest, TupleBinToPython) {
    using namespace tuplex;
    using namespace std;

    string test_type_str = "(Struct[(str,'id'=>i64),(str,'name'->str),(str,'url'->str)],str,Option[Struct[(str,'gravatar_id'->str),(str,'id'->i64),(str,'url'->str),(str,'avatar_url'->str),(str,'login'->str)]],boolean,str,Struct[(str,'action'=>str),(str,'actor'=>str),(str,'actor_gravatar'=>str),(str,'comment_id'=>i64),(str,'commit'=>str),(str,'desc'=>null),(str,'head'=>str),(str,'name'=>str),(str,'object'=>str),(str,'object_name'=>str),(str,'page_name'=>str),(str,'push_id'=>i64),(str,'ref'=>str),(str,'repo'=>str),(str,'sha'=>str),(str,'shas'=>List[List[str]]),(str,'size'=>i64),(str,'snippet'=>str),(str,'summary'=>null),(str,'target'=>Struct[(str,'gravatar_id'->str),(str,'repos'->i64),(str,'followers'->i64),(str,'login'->str)]),(str,'title'=>str),(str,'url'=>str)],Struct[(str,'gravatar_id'->str),(str,'id'->i64),(str,'url'->str),(str,'avatar_url'->str),(str,'login'->str)],str)";
           test_type_str = "(Struct[(str,'id'=>i64),(str,'name'->str),(str,'url'->str)],str,Option[Struct[(str,'gravatar_id'->str),(str,'id'->i64),(str,'url'->str),(str,'avatar_url'->str),(str,'login'->str)]],boolean,str,Struct[(str,'action'=>str),(str,'actor'=>str),(str,'actor_gravatar'=>str),(str,'desc'=>null),(str,'head'=>str),(str,'name'=>str),(str,'object'=>str),(str,'object_name'=>Option[str]),(str,'page_name'=>str),(str,'push_id'=>i64),(str,'ref'=>str),(str,'repo'=>str),(str,'sha'=>str),(str,'shas'=>List[List[str]]),(str,'size'=>i64),(str,'snippet'=>str),(str,'summary'=>null),(str,'target'=>Struct[(str,'gravatar_id'->str),(str,'repos'->i64),(str,'followers'->i64),(str,'login'->str)]),(str,'title'=>str),(str,'url'=>str)],Struct[(str,'gravatar_id'->str),(str,'id'->i64),(str,'url'->str),(str,'avatar_url'->str),(str,'login'->str)],str)";
                auto ref = "(Struct[(str,'id'=>i64),(str,'name'->str),(str,'url'->str)],str,Option[Struct[(str,'gravatar_id'->str),(str,'id'->i64),(str,'url'->str),(str,'avatar_url'->str),(str,'login'->str)]],boolean,str,Struct[(str,'action'=>str),(str,'actor'=>str),(str,'actor_gravatar'=>str),(str,'desc'=>null),(str,'head'=>str),(str,'name'=>str),(str,'object'=>str),(str,'object_name'=>Option[str]),(str,'page_name'=>str),(str,'push_id'=>i64),(str,'ref'=>str),(str,'repo'=>str),(str,'sha'=>str),(str,'shas'=>List[List[str]]),(str,'size'=>i64),(str,'snippet'=>str),(str,'summary'=>null),(str,'target'=>Struct[(str,'gravatar_id'->str),(str,'repos'->i64),(str,'followers'->i64),(str,'login'->str)]),(str,'title'=>str),(str,'url'=>str)],Struct[(str,'gravatar_id'->str),(str,'id'->i64),(str,'url'->str),(str,'avatar_url'->str),(str,'login'->str)],str)";
    EXPECT_EQ(ref, test_type_str);
    auto test_type = python::Type::decode(test_type_str);
    ASSERT_TRUE(test_type.isTupleType());
    EXPECT_EQ(test_type.parameters().size(), 8);
    std::cout<<"testing with tuple..."<<std::endl;

    // fetch test-line
    // load all lines from github.json, go over and check rc
    auto lines = splitToLines(fileToString("../resources/ndjson/github.json"));

   // auto line = lines[19];

    runtime::init(ContextOptions::defaults().RUNTIME_LIBRARY().toPath());

    JITCompiler jit;

    // create parse function (easier to debug!)
    std::vector<std::string> columns({"repo", "type", "org", "public", "created_at", "payload", "actor", "id"});
    auto f = codegen::generateJsonTupleTestParse(jit, test_type, columns);
    ASSERT_TRUE(f);

    unsigned line_no = 0;
    for(auto line : lines) {
        uint8_t* buf = nullptr;
        size_t size = 0;

        auto rc = f(reinterpret_cast<const uint8_t*>(line.c_str()), line.size(), &buf, &size);
        if(rc == 70) {
            std::cout<<line_no<<": "<<"BADPARSE_STRING_INPUT"<<std::endl;
            line_no++;
            continue;
        }

        EXPECT_EQ(rc, 0);

        Row row = Row::fromMemory(Schema(Schema::MemoryLayout::ROW, test_type), buf, size);
        std::cout<<line_no<<": "<<row.toPythonString()<<std::endl;
        line_no++;
    }
}


TEST_F(JsonTuplexTest, BinToPython) {
    using namespace tuplex;
    using namespace std;

    auto test_type_str = "(Struct[(str,'actor'->Struct[(str,'gravatar_id'->str),(str,'id'->i64),(str,'url'->str),(str,'avatar_url'->str),(str,'login'->str)]),(str,'created_at'->str),(str,'id'->str),(str,'org'=>Struct[(str,'gravatar_id'->str),(str,'id'->i64),(str,'url'->str),(str,'avatar_url'->str),(str,'login'->str)]),(str,'payload'->Struct[(str,'action'=>str),(str,'actor'=>str),(str,'actor_gravatar'=>str),(str,'comment_id'=>i64),(str,'commit'=>str),(str,'desc'=>null),(str,'head'=>str),(str,'name'=>str),(str,'object'=>str),(str,'object_name'=>Option[str]),(str,'page_name'=>str),(str,'push_id'=>i64),(str,'ref'=>str),(str,'repo'=>str),(str,'sha'=>str),(str,'shas'=>List[List[str]]),(str,'size'=>i64),(str,'snippet'=>str),(str,'summary'=>null),(str,'title'=>str),(str,'url'=>str)]),(str,'public'->boolean),(str,'repo'->Struct[(str,'id'=>i64),(str,'name'->str),(str,'url'->str)]),(str,'type'->str)])";

    auto type = python::Type::decode(test_type_str);

    auto dict_type = type.parameters().front();

    runtime::init(ContextOptions::defaults().RUNTIME_LIBRARY().toPath());

    JITCompiler jit;
    auto f = codegen::generateJsonTestParse(jit, dict_type);

    // call f
    size_t size = 0;
    uint8_t* buf = nullptr;

    // second setup with shas
    // test setup.
    {
        // load all lines from github.json, go over and check rc
        auto lines = splitToLines(fileToString("../resources/ndjson/github.json"));

        auto line = lines[0]; // <-- should have sha list
        auto rc = f(reinterpret_cast<const uint8_t*>(line.c_str()), line.size(), &buf, &size);
        EXPECT_EQ(rc, 0);

        // now decode
        auto json_str = decodeStructDictFromBinary(dict_type, buf, size);
        std::cout<<json_str<<std::endl;

        // convert to python object
        python::lockGIL();

        auto py_obj = json_string_to_pyobject(json_str, dict_type);
        PyObject_Print(py_obj, stdout, 0); std::cout<<std::endl;
        python::unlockGIL();

//        for(auto line : lines) {
//            auto rc = f(reinterpret_cast<const uint8_t*>(line.c_str()), line.size(), &buf, &size);
//            EXPECT_EQ(rc, 0);
//        }

        //ASSERT_TRUE(size > 0);

//        // now decode
//        auto json_str = decodeStructDictFromBinary(dict_type, buf, size);
//        std::cout<<json_str<<std::endl;
//
//        // convert to python object
//        python::lockGIL();
//
//        auto py_obj = json_string_to_pyobject(json_str, dict_type);
//        PyObject_Print(py_obj, stdout, 0); std::cout<<std::endl;
//        python::unlockGIL();
    }

//    // test setup.
//    {
//        // input buffer and size
//        char input_buf[] = "{\"repo\":{\"id\":1357116,\"url\":\"https://api.github.dev/repos/ezmobius/super-nginx\",\"name\":\"ezmobius/super-nginx\"},\"type\":\"WatchEvent\",\"public\":true,\"created_at\":\"2011-02-12T00:00:06Z\",\"payload\":{\"repo\":\"ezmobius/super-nginx\",\"actor\":\"sosedoff\",\"actor_gravatar\":\"cd73497eb3c985f302723424c3fa5b50\",\"action\":\"started\"},\"actor\":{\"gravatar_id\":\"cd73497eb3c985f302723424c3fa5b50\",\"id\":71051,\"url\":\"https://api.github.dev/users/sosedoff\",\"avatar_url\":\"https://secure.gravatar.com/avatar/cd73497eb3c985f302723424c3fa5b50?d=http://github.dev%2Fimages%2Fgravatars%2Fgravatar-user-420.png\",\"login\":\"sosedoff\"},\"id\":\"1127195541\"}";
//        size_t input_buf_size = strlen(input_buf) + 1;
//        auto rc = f(reinterpret_cast<const uint8_t*>(input_buf), input_buf_size, &buf, &size);
//        EXPECT_EQ(rc, 0);
//        ASSERT_TRUE(size > 0);
//
//        // now decode
//        auto json_str = decodeStructDictFromBinary(dict_type, buf, size);
//        std::cout<<json_str<<std::endl;
//
//        // convert to python object
//        python::lockGIL();
//
//        auto py_obj = json_string_to_pyobject(json_str, dict_type);
//        PyObject_Print(py_obj, stdout, 0); std::cout<<std::endl;
//        python::unlockGIL();
//    }



//    // load data from file
//    auto data = fileToString(string("../resources/struct_dict_data.bin"));
//    EXPECT_EQ(data.size(), 726); // size
//
//    // decode
//    auto ptr = reinterpret_cast<const uint8_t*>(data.c_str());
//
//    uint32_t offset = *((uint64_t*)ptr) & 0xFFFFFFFF;
//    uint32_t size = *((uint64_t*)ptr) >> 32u;
//
//    std::cout<<"offset: "<<offset<<" size: "<<size<<std::endl;
//
//    auto start_ptr = ptr + offset; // this is where the data starts
//
//    // for ref: this is what should be contained within the buffer:
//    // {"repo":{"id":1357116,"url":"https://api.github.dev/repos/ezmobius/super-nginx","name":"ezmobius/super-nginx"},"type":"WatchEvent","public":true,"created_at":"2011-02-12T00:00:06Z","payload":{"repo":"ezmobius/super-nginx","actor":"sosedoff","actor_gravatar":"cd73497eb3c985f302723424c3fa5b50","action":"started"},"actor":{"gravatar_id":"cd73497eb3c985f302723424c3fa5b50","id":71051,"url":"https://api.github.dev/users/sosedoff","avatar_url":"https://secure.gravatar.com/avatar/cd73497eb3c985f302723424c3fa5b50?d=http://github.dev%2Fimages%2Fgravatars%2Fgravatar-user-420.png","login":"sosedoff"},"id":"1127195541"}
//    // dump buffer
//    core::asciidump(std::cout, start_ptr, size, true);
//
//    // now decode
//    auto json_str = decodeStructDictFromBinary(type.parameters().front(), start_ptr, size);
//
//    std::cout<<json_str<<std::endl;
//
//    ASSERT_TRUE(!json_str.empty());
}

TEST_F(JsonTuplexTest, GithubLoadTakeTop) {
    using namespace tuplex;
    using namespace std;

    auto opt = microTestOptions();
    opt.set("tuplex.executorCount", "0"); // start single-threaded
    opt.set("tuplex.resolveWithInterpreterOnly", "false");
    Context ctx(opt);
    bool unwrap_first_level = true;

    // check ref file lines
    string ref_path = "../resources/ndjson/github.json";
    auto lines = splitToLines(fileToString(ref_path));

    // check both unwrap and no unwrap
    auto v1 = ctx.json(ref_path, false).withColumn("test", UDF("lambda row: row['payload'].get('repo')")).takeAsVector(5);
    EXPECT_EQ(v1.size(), 5);
}

TEST_F(JsonTuplexTest, PushEventNumberOfCommits) {
    using namespace tuplex;
    using namespace std;

    auto opt = microTestOptions();
    opt.set("tuplex.executorCount", "0"); // start single-threaded
    opt.set("tuplex.resolveWithInterpreterOnly", "false");
    Context ctx(opt);
    bool unwrap_first_level = true;

    // check ref file lines
    string ref_path = "../resources/hyperspecialization/github_daily/*.json.sample";
    auto lines = splitToLines(fileToString(ref_path));

    std::cout<<ctx.json(ref_path, true).columns()<<std::endl;

    // TODO: can add expensive string operation to produce and format a condensed commit message for the push event!
    // => easy! I.e., sort by committer and timestamp? could use other dict functions for this??
    auto v1 = ctx.json(ref_path, true)
                 .withColumn("commits", UDF("lambda row: row['payload'].get('commits')"))
                 .withColumn("number_of_commits", UDF("lambda row: len(row['commits']) if row['commits'] else 0"))
                 .filter(UDF("lambda row: row['type'] == 'PushEvent'"))
                 .selectColumns(std::vector<std::string>{"type", "number_of_commits"})
                 .takeAsVector(5);
    EXPECT_EQ(v1.size(), 5);
}

TEST_F(JsonTuplexTest, GithubLoad) {
    using namespace tuplex;
    using namespace std;

    auto opt = microTestOptions();
    opt.set("tuplex.executorCount", "0"); // start single-threaded
    opt.set("tuplex.resolveWithInterpreterOnly", "false");
    Context ctx(opt);
    bool unwrap_first_level = true;

    // check ref file lines
    string ref_path = "../resources/ndjson/github.json";
    auto lines = splitToLines(fileToString(ref_path));
    auto ref_row_count = lines.size();

    // bug in here, presence map doesn't get serialized??
    {
        // first test -> simple load in both unwrap and no unwrap mode.
        unwrap_first_level = true;
        auto& ds = ctx.json(ref_path, unwrap_first_level);
        // no columns
        EXPECT_TRUE(!ds.columns().empty());
        auto v = ds.collectAsVector();

        EXPECT_EQ(v.size(), ref_row_count);
    }


    // this here works:
    {
        // first test -> simple load in both unwrap and no unwrap mode.
        unwrap_first_level = false;
        auto& ds = ctx.json(ref_path, unwrap_first_level);
        // no columns
        EXPECT_TRUE(ds.columns().empty());
        auto v = ds.collectAsVector();

        EXPECT_EQ(v.size(), ref_row_count);
    }

//    auto& ds = ctx.json("../resources/ndjson/github_two_rows.json", unwrap_first_level);
    //// could extract columns via keys() or so? -> no support for this yet.
    // ds.map(UDF("lambda x: (x['type'], int(x['id']))")).show();
    //ds.show();


    //std::cout<<"json malloc report:\n"<<codegen::JsonMalloc_Report()<<std::endl;

    //ds.map(UDF("lambda x: x['type']")).unique().show();

    // Next steps: -> should be able to load any github file (start with the small samples)
    //             -> use maybe show(5) to speed things up (--> sample manipulation?)
    //             -> add Python API to tuplex for dealing with JSON?
    //             -> design example counting events across files maybe?
    //             -> what about pushdown then?

//    // simple func --> this works only with unwrapping!
// unwrap_first_level = true;
//    ctx.json("../resources/ndjson/github.json", unwrap_first_level)
//       .filter(UDF("lambda x: x['type'] == 'PushEvent'"))
//       .mapColumn("id", UDF("lambda x: int(x)"))
//       .selectColumns(std::vector<std::string>({"repo", "type", "id"}))
//       .show();

    // @TODO: tests
    // -> unwrap should be true and the basic show pipeline work as well.
    // -> a decode of stored struct dict/list into JSON would be cool.
    // -> a decode of struct dict as python would be cool.
    // -> assigning to dictionaries, i.e. the example below working would be an improvement.
    // finally, run the show (with limit!) example on ALL json data for the days to prove it works.
}

TEST_F(JsonTuplexTest, StructAndFT) {
    using namespace tuplex;
    using namespace std;
    using namespace tuplex::codegen;
    using namespace llvm;

    LLVMEnvironment env;

    auto& ctx = env.getContext();
    auto t = python::Type::decode("(Struct[(str,'column1'->Struct[(str,'a'->i64),(str,'b'->i64),(str,'c'->Option[i64])])])");
    FlattenedTuple ft(&env);
    ft.init(t);
    auto F = getOrInsertFunction(env.getModule().get(), "test_func", ctypeToLLVM<int64_t>(ctx), ft.getLLVMType());
    auto bEntry = BasicBlock::Create(ctx, "entry", F);

    llvm::IRBuilder<> builder(bEntry);
    builder.CreateRet(env.i64Const(0));
    auto ir = moduleToString(*env.getModule().get());

    std::cout<<"\n"<<ir<<std::endl;
    EXPECT_TRUE(ir.size() > 0);

}

TEST_F(JsonTuplexTest, TupleFlattening) {
    using namespace tuplex;
    using namespace std;
    std::string type_str = "(Struct[(str,'id'->i64),(str,'url'->str),(str,'name'->str)],str,Option[Struct[(str,'gravatar_id'->str),(str,'id'->i64),(str,'url'->str),(str,'avatar_url'->str),(str,'login'->str)]],boolean,str,Struct[(str,'shas'->List[List[str]]),(str,'repo'->str),(str,'actor'->str),(str,'ref'->str),(str,'size'->i64),(str,'head'->str),(str,'actor_gravatar'->str),(str,'push_id'->i64)],Struct[(str,'gravatar_id'->str),(str,'id'->i64),(str,'url'->str),(str,'avatar_url'->str),(str,'login'->str)],str)";

    auto type = python::Type::decode(type_str);

    // proper flattening needs to be achieved.
    auto tree = TupleTree<codegen::SerializableValue>(type);

    // make sure no element is of option type?
    for(unsigned i = 0; i < tree.numElements(); ++i)
        std::cout<<"field "<<i<<": "<<tree.fieldType(i).desc()<<std::endl;
    ASSERT_GT(tree.numElements(), 0);
}


namespace tuplex {
    DataSet& github_pipeline(Context& ctx, const std::string& pattern) {
        // in order to extract repo -> for 2012 till 2014 incl. repo key is called repository!
        // => unknown key will trigger badparseinput exception.

        auto repo_id_code = "def extract_repo_id(row):\n"
                            "\tif 2012 <= row['year'] <= 2014:\n"
                            "\t\treturn row['repository']['id']\n"
                            "\telse:\n"
                            "\t\treturn row['repo']['id']\n";

        return ctx.json(pattern)
                  .withColumn("year", UDF("lambda x: int(x['created_at'].split('-')[0])"))
                  .withColumn("repo_id", UDF(repo_id_code))
                  .filter(UDF("lambda x: x['type'] == 'ForkEvent'"))
                  .selectColumns(std::vector<std::string>({"type", "repo_id", "year"}));
    }
}

TEST_F(JsonTuplexTest, ListOfTuples) {
    // create a simple example with loadToHeapPtr!
    using namespace llvm;
    using namespace tuplex;
    using namespace tuplex::codegen;

    LLVMEnvironment env;

    std::string func_name = "test_tuples";

    // create func
    auto& ctx = env.getContext();
    auto func = getOrInsertFunction(env.getModule().get(), func_name, ctypeToLLVM<int64_t>(ctx), ctypeToLLVM<uint8_t*>(ctx),
                                    ctypeToLLVM<int64_t>(ctx));

    auto argMap = mapLLVMFunctionArgs(func, {"buf", "buf_size"});

    BasicBlock* bBody = BasicBlock::Create(ctx, "entry", func);
    IRBuilder<> builder(bBody);

    FlattenedTuple ft(&env);

    auto tuple_type = python::Type::makeTupleType({python::Type::STRING, python::Type::BOOLEAN});
    ft.init(tuple_type);
    // set elements
    ft.setElement(builder, 0, argMap["buf"], argMap["buf_size"], env.i1Const(false));
    ft.setElement(builder, 1, env.boolConst(true), env.i64Const(8), env.i1Const(false));

    auto tuple_ptr = ft.loadToHeapPtr(builder);

    // create list now
    auto list_type = python::Type::makeListType(tuple_type);

    auto list_llvm_type = env.getOrCreateListType(list_type);
    auto list_ptr = env.CreateFirstBlockAlloca(builder, list_llvm_type);

    list_reserve_capacity(env, builder, list_ptr, list_type, env.i64Const(10), true);

    // store tuple
    list_store_value(env, builder, list_ptr, list_type, env.i64Const(9), SerializableValue(tuple_ptr, nullptr, nullptr));

    // now get element from there.
    auto tuple = builder.CreateLoad(tuple_ptr);
    FlattenedTuple ft_check = FlattenedTuple::fromLLVMStructVal(&env, builder, tuple, tuple_type);

    llvm::Value* size = ft_check.getSize(0);
    builder.CreateRet(size);

    runtime::init(ContextOptions::defaults().RUNTIME_LIBRARY().toPath());

    JITCompiler jit;

    // use optimizer as well
    LLVMOptimizer opt; opt.optimizeModule(*env.getModule());

    jit.compile(std::move(env.getModule()));

    auto f = reinterpret_cast<int64_t(*)(const char*,size_t)>(jit.getAddrOfSymbol(func_name));

    std::string test_str = "Hello world";
    auto rc = f(test_str.c_str(), test_str.size() + 1);
    EXPECT_EQ(rc, test_str.size() + 1);
}

TEST_F(JsonTuplexTest, LoadListOfTuplesLoad) {
    // path
    auto pattern = "../resources/list_of_tuples.json";

    using namespace tuplex;
    using namespace std;

    auto co = ContextOptions::defaults();

    // deactivate pushdown
    co.set("tuplex.optimizer.selectionPushdown", "false");

    // deactivate filter pushdown as well...
    co.set("tuplex.optimizer.filterPushdown", "false");


    // important !!!
    co.set("tuplex.useLLVMOptimizer", "true");

    co.set("tuplex.executorCount", "0");

    Context c(co);
    c.json(pattern).withColumn("new_col", UDF("lambda x: str(x['rowno'])")).show();
}

// bbsn00
TEST_F(JsonTuplexTest, SampleForAllFiles) {
    using namespace tuplex;
    using namespace std;

    auto co = ContextOptions::defaults();

    // deactivate pushdown
    co.set("tuplex.optimizer.selectionPushdown", "false");

    // deactivate filter pushdown as well...
    co.set("tuplex.optimizer.filterPushdown", "false");

    co.set("tuplex.useLLVMOptimizer", "true");

    co.set("tuplex.executorCount", "0");

    Context c(co);

    // /hot/data/github_daily/*.json
    string path = "/hot/data/github_daily/*.json";

    // use sample
    string pattern = "../resources/hyperspecialization/github_daily/*.json.sample";

    // pattern = "../resources/hyperspecialization/github_daily/2012*.json.sample"; // <-- put in here any problematic files found.
     // pattern = "../resources/hyperspecialization/github_daily/2013-10-15.json.sample";

    std::cout<<"Processing in global mode..."<<std::endl;

    auto& ds = github_pipeline(c, pattern);
    EXPECT_FALSE(ds.isError());
    ds.tocsv("github_forkevents.csv");

    std::cout<<"Processing in hyper mode..."<<std::endl;

    // process each file on its own and compare to the global files...
    // -> they should be identical... (up to order)
    auto paths = glob(pattern);
    for(const auto& path : paths) {
        std::cout<<"--> processing path "<<path<<std::endl;

        auto basename = path.substr(path.rfind("/") + 1);
        auto output_path = basename.substr(0, basename.find('.')) + "_github_forkevents.csv";
        std::cout<<"writing output to: "<<output_path<<std::endl;

        auto& ds = github_pipeline(c, path);
        EXPECT_FALSE(ds.isError());
        ds.tocsv(output_path);
    }
}

TEST_F(JsonTuplexTest, FilterPromoDev) {

    using namespace std;
    using namespace tuplex;

    auto co = ContextOptions::defaults();

    // deactivate pushdown
    co.set("tuplex.optimizer.selectionPushdown", "false");

    // deactivate filter pushdown as well...
    co.set("tuplex.optimizer.filterPushdown", "false");
    co.set("tuplex.optimizer.filterPromotion", "true");
    co.set("tuplex.useLLVMOptimizer", "true");
    co.set("tuplex.executorCount", "0");
    Context c(co);

    string pattern = "../resources/hyperspecialization/github_daily/*.json.sample";
    auto& ds = github_pipeline(c, pattern);
    ds.tocsv("github_forkevents.csv");
}

TEST_F(JsonTuplexTest, MiniSampleForAllFiles) {
    using namespace tuplex;
    using namespace std;

    auto co = ContextOptions::defaults();

    // deactivate pushdown
    co.set("tuplex.optimizer.selectionPushdown", "false");

    // deactivate filter pushdown as well...
    co.set("tuplex.optimizer.filterPushdown", "false");

    Context c(co);

    auto path = "../resources/ndjson/github.json";

    // process all files (no hyperspecialization yet)
//    c.json(path).withColumn("repo_id", UDF("lambda x: x['repo']['id']"))
//     .filter(UDF("lambda x: x['type'] == 'ForkEvent'"))
//     .withColumn("year", UDF("lambda x: int(x['created_at'].split('-')[0])"))
//     .selectColumns(std::vector<std::string>({"type", "repo_id", "year"})).show(5);

    c.json(path).withColumn("repo_id", UDF("lambda x: x['repo']['id']"))
            .filter(UDF("lambda x: x['type'] == 'ForkEvent'"))
            .withColumn("year", UDF("lambda x: int(x['created_at'].split('-')[0])"))
            .selectColumns(std::vector<std::string>({"type", "repo_id", "year"}))
            .tocsv("github_forkevents.csv");
}

TEST_F(JsonTuplexTest, GZipFileRead) {
    using namespace tuplex;
    using namespace std;

    // check whether a file is gzip or not (using magic)
    auto gzip_data = fileToString(URI("../resources/gzip/basic.txt.gz"));

    // magic?
    cout<<"length: "<<gzip_data.size()<<endl;

    EXPECT_TRUE(is_gzip_file(reinterpret_cast<const uint8_t*>(gzip_data.c_str())));

    auto data = gzip::decompress(gzip_data.c_str(), gzip_data.size());
    EXPECT_EQ(data, "Hello world!\n");

    // use now VirtualFileSystem to process gzip file
    auto vf = VirtualFileSystem::open_file(URI("../resources/gzip/basic.txt.gz"), VirtualFileMode::VFS_READ);


    vf->close();
}

namespace tuplex {
    // helper func to decvelop parsing
    int64_t json_dummy_functor(void* userData, const uint8_t* buf, int64_t buf_size,
                               int64_t* out_normal_rows, int64_t* out_bad_rows, int8_t is_last_part) {
        using namespace codegen;

        // open up JSON parsing
        auto j = JsonParser_init();
        if(!j)
            return -1;

        int64_t row_number = 0;

        JsonParser_open(j, reinterpret_cast<const char *>(buf), buf_size);
        while (JsonParser_hasNextRow(j)) {
            if (JsonParser_getDocType(j) != JsonParser_objectDocType()) {
                // BADPARSE_STRINGINPUT
                auto line = JsonParser_getMallocedRow(j);
                free(line);
            }

            auto doc = *j->it;
            JsonItem *obj = nullptr;
            uint64_t rc = JsonParser_getObject(j, &obj);
            if (rc != 0)
                break; // --> don't forget to release stuff here!

            // release all allocated things
            JsonItem_Free(obj);

            runtime::rtfree_all();
            row_number++;
            JsonParser_moveToNextRow(j);
        }

        size_t size = j->stream.size_in_bytes();
        size_t truncated_bytes = j->stream.truncated_bytes();

        JsonParser_close(j);
        JsonParser_free(j);

        if(out_normal_rows)
            *out_normal_rows = row_number;
        if(out_bad_rows)
            *out_bad_rows = 0;

        // returns how many bytes are parsed...
        return buf_size - truncated_bytes;
    }
}

TEST_F(JsonTuplexTest, FilePartitioning) {
    using namespace tuplex;
    using namespace std;

    // check that split works
    auto input_split_size = memStringToSize("256MB");

    // need to init AWS SDK...
#ifdef BUILD_WITH_AWS
    {
        // init AWS SDK to get access to S3 filesystem
        auto& logger = Logger::instance().logger("aws");
        auto aws_credentials = AWSCredentials::get();
        auto options = ContextOptions::defaults();
        Timer timer;
        bool aws_init_rc = initAWS(aws_credentials, options.AWS_NETWORK_SETTINGS(), options.AWS_REQUESTER_PAY());
        logger.debug("initialized AWS SDK in " + std::to_string(timer.time()) + "s");
    }
#endif

    URI uri("s3://tuplex-public/data/github_daily/2018-10-15.json");
    size_t uri_size = 0;
    auto vfs = VirtualFileSystem::fromURI(uri);
    vfs.file_size(uri, reinterpret_cast<uint64_t&>(uri_size));
    std::cout<<"found uri "<<uri.toString()<<" of size "<<uri_size<<" ("<<sizeToMemString(uri_size)<<")"<<std::endl;
    size_t cur_size = 0;
    size_t total_rows = 0;
    size_t num_parts = 0;
    size_t partition_size = ContextOptions::defaults().PARTITION_SIZE();
    auto reader = std::make_unique<JsonReader>(nullptr, json_dummy_functor, partition_size);
    while(cur_size < uri_size) {
        auto rangeStart = cur_size;
        auto rangeEnd = std::min(cur_size + input_split_size, uri_size);
        cur_size += input_split_size;
        std::cout<<uri.toString()<<":"<<rangeStart<<"-"<<rangeEnd<<std::endl;

        std::cout<<"part "<<num_parts<<": start read..."<<std::endl;
        reader->setRange(rangeStart, rangeEnd);
        reader->read(uri);
        std::cout<<"done, read "<<pluralize(reader->inputRowCount(), "row")<<std::endl;
        total_rows += reader->inputRowCount();
        std::cout<<"total rows: "<<total_rows<<std::endl;
        reader.reset(new JsonReader(nullptr, json_dummy_functor, partition_size));
        num_parts++;
    }

    EXPECT_EQ(num_parts, 19);
    EXPECT_EQ(total_rows, 1522655);
    // make sure it's correct with expected file count
}


// some UDF examples that should work:
// x = {}
// x['test'] = 10 # <-- type of x is now Struct['test' -> i64]
// x['blub'] = {'a' : 20, 'b':None} # <-- type of x is now Struct['test' -> i64, 'blub' -> Struct['a' -> i64, 'b' -> null]]
