
CREATE EXTERNAL TABLE gh_rc2 (
   coordinates struct <
      coordinates: array <double>,
      type: string>,
   created_at string,
   entities struct <
      hashtags: array <struct <text: string>>,
      media: array <struct <
            display_url: string,
            expanded_url: string,
            media_url: string,
            media_url_https: string,
            sizes: struct <
               large: struct <
                  h: int,
                  resize: string,
                  w: int>,
               medium: struct <
                  h: int,
                  resize: string,
                  w: int>,
               small: struct <
                  h: int,
                  resize: string,
                  w: int>,
               thumb: struct <
                  h: int,
                  resize: string,
                  w: int>>,
            type: string,
            url: string>>,
      urls: array <struct <
            display_url: string,
            expanded_url: string,
            url: string>>,
      user_mentions: array <struct <
            name: string,
            screen_name: string>>>,
   geo struct <
      coordinates: array <double>,
      type: string>,
   id_str string,
   in_reply_to_screen_name string,
   in_reply_to_status_id_str string,
   in_reply_to_user_id_str string,
   place struct <
      attributes: struct <
         locality: string,
         region: string,
         street_address: string>,
      bounding_box: struct <
         coordinates: array <array <array <double>>>,
         type: string>,
      country: string,
      country_code: string,
      full_name: string,
      name: string,
      place_type: string,
      url: string>,
   possibly_sensitive boolean,
   retweeted_status struct <
      coordinates: struct <
         coordinates: array <double>,
         type: string>,
      created_at: string,
      entities: struct <
         hashtags: array <struct <
               text: string>>,
         media: array <struct <
               display_url: string,
               expanded_url: string,
               media_url: string,
               media_url_https: string,
               sizes: struct <
                  large: struct <
                     h: int,
                     resize: string,
                     w: int>,
                  medium: struct <
                     h: int,
                     resize: string,
                     w: int>,
                  small: struct <
                     h: int,
                     resize: string,
                     w: int>,
                  thumb: struct <
                     h: int,
                     resize: string,
                     w: int>>,
               type: string,
               url: string>>,
         urls: array <struct <
               display_url: string,
               expanded_url: string,
               url: string>>,
         user_mentions: array <struct <
               name: string,
               screen_name: string>>>,
      favorited: boolean,
      geo: struct <
         coordinates: array <double>,
         type: string>,
      id_str: string,
      in_reply_to_screen_name: string,
      in_reply_to_status_id_str: string,
      in_reply_to_user_id_str: string,
      place: struct <
         attributes: struct <
         locality: string,
         region: string,
         street_address: string
         >,
         bounding_box: struct <
            coordinates: array <array <array <double>>>,
            type: string>,
         country: string,
         country_code: string,
         full_name: string,
         name: string,
         place_type: string,
         url: string>,
      possibly_sensitive: boolean,
      scopes: struct <
         followers: boolean>,
      source: string,
      text: string,
      truncated: boolean,
      user: struct <
         contributors_enabled: boolean,
         created_at: string,
         default_profile: boolean,
         default_profile_image: boolean,
         description: string,
         favourites_count: int,
         followers_count: int,
         friends_count: int,
         geo_enabled: boolean,
         id_str: string,
         is_translator: boolean,
         lang: string,
         listed_count: int,
         `location`: string,
         name: string,
         profile_background_color: string,
         profile_background_image_url: string,
         profile_background_image_url_https: string,
         profile_background_tile: boolean,
         profile_banner_url: string,
         profile_image_url: string,
         profile_image_url_https: string,
         profile_link_color: string,
         profile_sidebar_border_color: string,
         profile_sidebar_fill_color: string,
         profile_text_color: string,
         profile_use_background_image: boolean,
         protected: boolean,
         screen_name: string,
         statuses_count: int,
         time_zone: string,
         url: string,
         utc_offset: int,
         verified: boolean>>,
   source string,
   text string,
   truncated boolean,
   user struct <
      contributors_enabled: boolean,
      created_at: string,
      default_profile: boolean,
      default_profile_image: boolean,
      description: string,
      favourites_count: int,
      followers_count: int,
      friends_count: int,
      geo_enabled: boolean,
      id_str: string,
      is_translator: boolean,
      lang: string,
      listed_count: int,
      `location`: string,
      name: string,
      profile_background_color: string,
      profile_background_image_url: string,
      profile_background_image_url_https: string,
      profile_background_tile: boolean,
      profile_banner_url: string,
      profile_image_url: string,
      profile_image_url_https: string,
      profile_link_color: string,
      profile_sidebar_border_color: string,
      profile_sidebar_fill_color: string,
      profile_text_color: string,
      profile_use_background_image: boolean,
      protected: boolean,
      screen_name: string,
      statuses_count: int,
      time_zone: string,
      url: string,
      utc_offset: int,
      verified: boolean>
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS rcfile
LOCATION '/user/ahanna/gh_rc2';