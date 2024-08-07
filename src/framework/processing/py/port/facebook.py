
"""
DDP facebook module

This module contains functions to handle *.jons files contained within a facebook ddp
"""
from pathlib import Path
from typing import Any
import math
import logging
import zipfile
import re

import pandas as pd

import port.unzipddp as unzipddp
import port.helpers as helpers
from port.validate import (
    DDPCategory,
    StatusCode,
    ValidateInput,
    Language,
    DDPFiletype,
)

logger = logging.getLogger(__name__)

DDP_CATEGORIES = [
    DDPCategory(
        id="json_en",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.EN,
        known_files=[
"subscription_for_no_ads.json", "other_categories_used_to_reach_you.json", "ads_feedback_activity.json", "ads_personalization_consent.json", "advertisers_you've_interacted_with.json", "advertisers_using_your_activity_or_information.json", "story_views_in_past_7_days.json", "ad_preferences.json", "groups_you've_searched_for.json", "your_search_history.json", "primary_public_location.json", "timezone.json", "primary_location.json", "your_privacy_jurisdiction.json", "people_and_friends.json", "ads_interests.json", "notifications.json", "notification_of_meta_privacy_policy_update.json", "recently_viewed.json", "recently_visited.json", "your_avatar.json", "meta_avatars_post_backgrounds.json", "contacts_sync_settings.json", "timezone.json", "autofill_information.json", "profile_information.json", "profile_update_history.json", "your_transaction_survey_information.json", "your_recently_followed_history.json", "your_recently_used_emojis.json", "no-data.txt", "navigation_bar_activity.json", "pages_and_profiles_you_follow.json", "pages_you've_liked.json", "your_saved_items.json", "fundraiser_posts_you_likely_viewed.json", "your_fundraiser_donations_information.json", "your_event_responses.json", "event_invitations.json", "your_event_invitation_links.json", "likes_and_reactions_1.json", "your_uncategorized_photos.json", "payment_history.json", "no-data.txt", "your_answers_to_membership_questions.json", "your_group_membership_activity.json", "your_contributions.json", "group_posts_and_comments.json", "your_comments_in_groups.json", "instant_games.json", "your_page_or_groups_badges.json", "instant_games_usage_data.json", "no-data.txt", "who_you've_followed.json", "people_you_may_know.json", "received_friend_requests.json", "your_friends.json",
        ],
    )
]

STATUS_CODES = [
    StatusCode(id=0, description="Valid DDP", message=""),
    StatusCode(id=1, description="Not a valid DDP", message=""),
    StatusCode(id=2, description="Bad zipfile", message=""),
]


def validate(zfile: Path) -> ValidateInput:
    """
    Validates the input of an Instagram zipfile
    """

    validation = ValidateInput(STATUS_CODES, DDP_CATEGORIES)

    try:
        paths = []
        with zipfile.ZipFile(zfile, "r") as zf:
            for f in zf.namelist():
                p = Path(f)
                if p.suffix in (".html", ".json"):
                    logger.debug("Found: %s in zip", p.name)
                    paths.append(p.name)

        if validation.infer_ddp_category(paths):
            validation.set_status_code_by_id(0)
        else:
            validation.set_status_code_by_id(1)

    except zipfile.BadZipFile:
        validation.set_status_code_by_id(2)

    return validation


#################################################################################################
# NEW CODE

def who_youve_followed_to_df(facebook_zip: str) -> pd.DataFrame:

    b = unzipddp.extract_file_from_zip(facebook_zip, "who_you've_followed.json")
    d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["following_v3"]  # pyright: ignore
        for item in items:
            datapoints.append((
                helpers.fix_latin1_string(item.get("name", "")),
                helpers.epoch_to_iso(item.get("timestamp", {}))
            ))
        out = pd.DataFrame(datapoints, columns=["Name", "Timestamp"])

    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out


def your_friends_to_df(facebook_zip: str) -> pd.DataFrame:

    b = unzipddp.extract_file_from_zip(facebook_zip, "your_friends.json")
    d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["friends_v2"]  # pyright: ignore
        datapoints.append((len(items)))

        out = pd.DataFrame(datapoints, columns=["Aantal vrienden op facebook"])

    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out



def ads_interests_to_df(facebook_zip: str) -> pd.DataFrame:

    b = unzipddp.extract_file_from_zip(facebook_zip, "ads_interests.json")
    d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["topics_v2"]  # pyright: ignore
        for item in items:
            datapoints.append((
                helpers.fix_latin1_string(item),
            ))
        out = pd.DataFrame(datapoints, columns=["Ad"])

    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out



def recently_viewed_to_df(facebook_zip: str) -> pd.DataFrame:
    b = unzipddp.extract_file_from_zip(facebook_zip, "recently_viewed.json")
    d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["recently_viewed"] # pyright: ignore
        for item in items:

            if "entries" in item:
                for entry in item["entries"]:
                    datapoints.append((
                        helpers.fix_latin1_string(item.get("name", "")),
                        helpers.fix_latin1_string(entry.get("data", {}).get("name", "")),
                        entry.get("data", {}).get("uri", ""),
                        helpers.epoch_to_iso(entry.get("timestamp", ""))
                    ))

            # The nesting goes deeper
            if "children" in item:
                for child in item["children"]:
                    for entry in child["entries"]:
                        datapoints.append((
                            helpers.fix_latin1_string(child.get("name", "")),
                            helpers.fix_latin1_string(entry.get("data", {}).get("name", "")),
                            entry.get("data", {}).get("uri", ""),
                            helpers.epoch_to_iso(entry.get("timestamp", ""))
                        ))

        datapoints_sorted = sorted(datapoints, key= lambda x: helpers.generate_key_for_sorting_from_timestamp_in_tuple(x, 3))
        out = pd.DataFrame(datapoints_sorted, columns=["Watched", "Name", "Link", "Date"])

    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out



def recently_visited_to_df(facebook_zip: str) -> pd.DataFrame:
    b = unzipddp.extract_file_from_zip(facebook_zip, "recently_visited.json")
    d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["visited_things_v2"]  # pyright: ignore
        for item in items:
            if "entries" in item:
                for entry in item["entries"]:
                    datapoints.append((
                        item.get("name", ""),
                        helpers.fix_latin1_string(entry.get("data", {}).get("name", "")),
                        entry.get("data", {}).get("uri", ""),
                        helpers.epoch_to_iso(entry.get("timestamp", ""))
                    ))

        datapoints_sorted = sorted(datapoints, key= lambda x: helpers.generate_key_for_sorting_from_timestamp_in_tuple(x, 3))
        out = pd.DataFrame(datapoints_sorted, columns=["Watched", "Name", "Link", "Date"])
        
    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out



def profile_information_to_df(facebook_zip: str) -> pd.DataFrame:
    b = unzipddp.extract_file_from_zip(facebook_zip, "profile_information.json")
    d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["profile_v2"]  # pyright: ignore
        datapoints.append((
            items.get("gender", {}).get("gender_option", ""),
            items.get("gender", {}).get("pronoun", ""),
            ", ".join(items.get("gender", {}).get("custom_genders", []))
        ))

        out = pd.DataFrame(datapoints, columns=["Gender", "Pronoun", "Custom genders"])
        
    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out



def profile_update_history_to_df(facebook_zip: str) -> pd.DataFrame:
    b = unzipddp.extract_file_from_zip(facebook_zip, "profile_update_history.json")
    d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["profile_updates_v2"]  # pyright: ignore
        for item in items:
            datapoints.append((
                helpers.fix_latin1_string(item.get("title", "")),
                helpers.epoch_to_iso(item.get("timestamp", ""))
            ))
        out = pd.DataFrame(datapoints, columns=["Title", "Timestamp"])

    except Exception as e:
        logger.error("Exception caught: %s", e)
    return out


def your_event_responses_to_df(facebook_zip: str) -> pd.DataFrame:

    b = unzipddp.extract_file_from_zip(facebook_zip, "your_event_responses.json")
    d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["event_responses_v2"]["events_joined"]  # pyright: ignore
        for item in items:
            datapoints.append((
                helpers.fix_latin1_string(item.get("name", "")),
                helpers.epoch_to_iso(item.get("start_timestamp", ""))
            ))
        out = pd.DataFrame(datapoints, columns=["Name", "Timestamp"])

    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out


def group_posts_and_comments_to_df(facebook_zip: str) -> pd.DataFrame:

    b = unzipddp.extract_file_from_zip(facebook_zip, "group_posts_and_comments.json")
    d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        l = d["group_posts_v2"]  # pyright: ignore
        for item in l:
            denested_dict = helpers.dict_denester(item)

            datapoints.append((
                helpers.fix_latin1_string(helpers.find_item(denested_dict, "title")),
                helpers.fix_latin1_string(helpers.find_item(denested_dict, "post")),
                helpers.epoch_to_iso(helpers.find_item(denested_dict, "timestamp")),
                helpers.find_item(denested_dict, "url"),
            ))


        datapoints_sorted = sorted(datapoints, key= lambda x: helpers.generate_key_for_sorting_from_timestamp_in_tuple(x, 2))
        out = pd.DataFrame(datapoints_sorted, columns=["Title", "Post", "Date", "Url"])
    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out



def your_answers_to_membership_questions_to_df(facebook_zip: str) -> pd.DataFrame:

    b = unzipddp.extract_file_from_zip(facebook_zip, "your_answers_to_membership_questions.json")
    d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
                  
        items = d["group_membership_questions_answers_v2"]["group_answers"]  # pyright: ignore
        for item in items:
            datapoints.append((
                helpers.fix_latin1_string(item.get("group_name", "")),
            ))
        out = pd.DataFrame(datapoints, columns=["Group name"])

    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out



def your_comments_in_groups_to_df(facebook_zip: str) -> pd.DataFrame:

    b = unzipddp.extract_file_from_zip(facebook_zip, "your_comments_in_groups.json")
    d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        l = d["group_comments_v2"]  # pyright: ignore
        for item in l:
            denested_dict = helpers.dict_denester(item)

            datapoints.append((
                helpers.fix_latin1_string(helpers.find_item(denested_dict, "title")),
                helpers.fix_latin1_string(helpers.find_item(denested_dict, "comment-comment")),
                helpers.fix_latin1_string(helpers.find_item(denested_dict, "group")),
                helpers.epoch_to_iso(helpers.find_item(denested_dict, "timestamp")),
            ))


        datapoints_sorted = sorted(datapoints, key= lambda x: helpers.generate_key_for_sorting_from_timestamp_in_tuple(x, 4))
        out = pd.DataFrame(datapoints_sorted, columns=["Title", "Comment", "Author", "Group", "Timestamp"])
    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out



def your_group_membership_activity_to_df(facebook_zip: str) -> pd.DataFrame:
    b = unzipddp.extract_file_from_zip(facebook_zip, "your_group_membership_activity.json")
    d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["groups_joined_v2"]  # pyright: ignore
        for item in items:
            denested_dict = helpers.dict_denester(item)

            datapoints.append((
                helpers.fix_latin1_string(helpers.find_item(denested_dict, "title")),
                helpers.fix_latin1_string(helpers.find_item(denested_dict, "name")),
                helpers.epoch_to_iso(helpers.find_item(denested_dict, "timestamp")),
            ))


        out = pd.DataFrame(datapoints, columns=["Title", "Group name", "Timestamp"])
        
    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out



def pages_and_profiles_you_follow_to_df(facebook_zip: str) -> pd.DataFrame:
    b = unzipddp.extract_file_from_zip(facebook_zip, "pages_and_profiles_you_follow.json")
    d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["pages_followed_v2"]  # pyright: ignore
        for item in items:
            datapoints.append((
                helpers.fix_latin1_string(item.get("title", "")),
                helpers.epoch_to_iso(item.get("timestamp", ""))
            ))

        out = pd.DataFrame(datapoints, columns=["Title", "Timestamp"])
        
    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out


def pages_youve_liked_to_df(facebook_zip: str) -> pd.DataFrame:
    b = unzipddp.extract_file_from_zip(facebook_zip, "pages_you've_liked.json")
    d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["page_likes_v2"]  # pyright: ignore
        for item in items:
            datapoints.append((
                helpers.fix_latin1_string(item.get("name", "")),
                item.get("url", ""),
                helpers.epoch_to_iso(item.get("timestamp", ""))
            ))

        out = pd.DataFrame(datapoints, columns=["Name", "Url", "Timestamp"])
        
    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out


def your_saved_items_to_df(facebook_zip: str) -> pd.DataFrame:
    b = unzipddp.extract_file_from_zip(facebook_zip, "your_saved_items.json")
    d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["saves_v2"]  # pyright: ignore
        for item in items:
            datapoints.append((
                helpers.fix_latin1_string(item.get("title", "")),
                helpers.epoch_to_iso(item.get("timestamp", ""))
            ))

        out = pd.DataFrame(datapoints, columns=["Title", "Timestamp"])
        
    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out


def your_search_history_to_df(facebook_zip: str) -> pd.DataFrame:
    b = unzipddp.extract_file_from_zip(facebook_zip, "your_search_history.json")
    d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["searches_v2"]  # pyright: ignore
        for item in items:
            denested_dict = helpers.dict_denester(item)

            datapoints.append((
                helpers.fix_latin1_string(helpers.find_item(denested_dict, "title")),
                helpers.fix_latin1_string(helpers.find_item(denested_dict, "text")),
                helpers.epoch_to_iso(helpers.find_item(denested_dict, "timestamp")),
            ))

        out = pd.DataFrame(datapoints, columns=["Title", "Text", "Timestamp"])
        
    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out


def comments_to_df(facebook_zip: str) -> pd.DataFrame:
    b = unzipddp.extract_file_from_zip(facebook_zip, "comments.json")
    d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["comments_v2"]  # pyright: ignore
        for item in items:
            denested_dict = helpers.dict_denester(item)

            datapoints.append((
                helpers.fix_latin1_string(helpers.find_item(denested_dict, "title")),
                helpers.fix_latin1_string(helpers.find_item(denested_dict, "comment-comment")),
                helpers.epoch_to_iso(helpers.find_item(denested_dict, "timestamp")),
            ))

        out = pd.DataFrame(datapoints, columns=["Title", "Comment", "Timestamp"])
        
    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out



def likes_and_reactions_to_df(instagram_zip: str) -> pd.DataFrame:
    """
    likes_and_reactions_x
    """

    out = pd.DataFrame()
    datapoints = []
    i = 1

    while True:
        b = unzipddp.extract_file_from_zip(instagram_zip, f"likes_and_reactions_{i}.json")
        d = unzipddp.read_json_from_bytes(b)

        if not d:
            break

        try:
            for item in d:
                denested_dict = helpers.dict_denester(item)

                datapoints.append((
                    helpers.fix_latin1_string(helpers.find_item(denested_dict, "title")),
                    helpers.fix_latin1_string(helpers.find_item(denested_dict, "reaction-reaction")),
                    helpers.epoch_to_iso(helpers.find_item(denested_dict, "timestamp")),
                ))

            i += 1

        except Exception as e:
            logger.error("Exception caught: %s", e)
            return pd.DataFrame()

    out = pd.DataFrame(datapoints, columns=["Title", "Reaction", "Timestamp"])

    return out



def your_comment_active_days_to_df(facebook_zip: str) -> pd.DataFrame:
    b = unzipddp.extract_file_from_zip(facebook_zip, "your_comment_active_days.json")
    d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["label_values"]  # pyright: ignore
        for item in items:
            datapoints.append((
                item.get("label", ""),
                item.get("value", ""),
            ))

        out = pd.DataFrame(datapoints, columns=["Label", "Value"])
        
    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out



def your_pages_to_df(facebook_zip: str) -> pd.DataFrame:
    b = unzipddp.extract_file_from_zip(facebook_zip, "your_pages.json")
    d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["pages_v2"]  # pyright: ignore
        for item in items:
            datapoints.append((
                helpers.fix_latin1_string(item.get("name", "")),
                item.get("url", ""),
                helpers.epoch_to_iso(item.get("timestamp", "")),
            ))

        out = pd.DataFrame(datapoints, columns=["Name", "Url", "Timestamp"])
        
    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out


# NOTE: WHICH FILE DO I NEED TO USE TO BASE THE GROUP EXTRACTION ON
# ANSWER: your_group_membership_activity.json
def groups_to_list(facebook_zip: str) -> list[str]:
    b = unzipddp.extract_file_from_zip(facebook_zip, "your_group_membership_activity.json")
    d = unzipddp.read_json_from_bytes(b)

    out = []

    try:
        items = d["groups_joined_v2"]  # pyright: ignore
        for item in items:
            denested_dict = helpers.dict_denester(item)

            out.append(
                helpers.fix_latin1_string(helpers.find_item(denested_dict, "name"))
            )
            out = list(set(out))
        
    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out

#####################################################################
# replace occurance in df

# Function to extract username
def get_username(facebook_zip: str) -> None | str:
    b = unzipddp.extract_file_from_zip(facebook_zip, "profile_information.json")
    d = unzipddp.read_json_from_bytes(b)

    username = None
    try:
        username = helpers.fix_latin1_string(d["profile_v2"]["name"]["full_name"])  # pyright: ignore
    except Exception as e:
        logger.error("Exception caught: %s", e)

    return username


# Function to extract emails
def get_emails(facebook_zip: str) ->  list[str]:
    b = unzipddp.extract_file_from_zip(facebook_zip, "profile_information.json")
    d = unzipddp.read_json_from_bytes(b)

    emails = []
    try:
        emails = d["profile_v2"]["emails"].get("emails", []) #pyright: ignore
    except Exception as e:
        logger.error("Exception caught: %s", e)

    return emails


def get_phone_numbers(facebook_zip: str) ->  list[str]:
    b = unzipddp.extract_file_from_zip(facebook_zip, "profile_information.json")
    d = unzipddp.read_json_from_bytes(b)

    out = []
    try:
        l = d["profile_v2"].get("phone_numbers", []) #pyright: ignore
        for d in l:
            out.append(d["phone_number"])
    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out 

def regex_substitution(value, pattern, replacement):
    if isinstance(value, str):  # Only apply substitution to strings
        try:
            return re.sub(pattern, replacement, value)
        except Exception:
            return value
    return value


def replace_in_df(df: pd.DataFrame, value: str | None, replacement: str) -> pd.DataFrame:
    """
    Usage: 
    username = get_username(/path_to_zip)
    replace_in_df(df, username, "You")
    """

    if value != None:
        pattern = rf"{value}"
        df = df.applymap(lambda x: regex_substitution(x, pattern, replacement))

    return df

