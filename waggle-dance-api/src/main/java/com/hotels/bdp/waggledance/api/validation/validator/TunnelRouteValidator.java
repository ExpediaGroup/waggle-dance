/**
 * Copyright (C) 2016-2017 Expedia Inc and hibernate-validator contributors.
 *
 * Based on:
 *
 * https://github.com/hibernate/hibernate-validator/blob/5.2/engine/src/main/java/org/hibernate/validator/internal/constraintvalidators/hv/EmailValidator.java#L33
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.bdp.waggledance.api.validation.validator;

import static java.util.regex.Pattern.CASE_INSENSITIVE;

import java.net.IDN;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

import org.hibernate.validator.internal.constraintvalidators.hv.EmailValidator;

import com.hotels.bdp.waggledance.api.validation.constraint.TunnelRoute;

/**
 * Inspired by Hibernate's {@link EmailValidator}, this class checks that a given character sequence (e.g.
 * {@link String}) is a well-formed list of hostnames, optionally preceded by a username, separated by "-&gt;", e.g.
 * "user1@host1 -&gt; host2".
 * <p>
 * These are some examples of valid expressions:
 * </p>
 * 
 * <pre>
 * my-host
 * another-host.co.uk
 * etluser@tenant-node.company.com
 * my-host -&gt; another-host.co.uk
 * another-host.co.uk -&gt; etluser@tenant-node.company.com
 * ec2-user@bastion-host -&gt; hadoop@emr-master
 * </pre>
 */
public class TunnelRouteValidator implements ConstraintValidator<TunnelRoute, CharSequence> {
  private static final String ATOM = "[a-z0-9!#$%&'*+/=?^_`{|}~-]";
  private static final String DOMAIN = "(([a-z0-9]|[a-z0-9][a-z0-9\\-]*[a-z0-9])\\.)*([a-z0-9]|[a-z0-9][a-z0-9\\-]*[a-z0-9])";
  private static final String IP_DOMAIN = "(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])";
  private static final int MAX_LOCAL_PART_LENGTH = 64;
  private static final int MAX_DOMAIN_PART_LENGTH = 255;

  /**
   * Regular expression for the local part (username) of a tunnel hop (everything before '@')
   */
  private static final Pattern LOCAL_PATTERN = java.util.regex.Pattern.compile(ATOM + "+(\\." + ATOM + "+)*",
      CASE_INSENSITIVE);

  /**
   * Regular expression for the domain part (hostname or IP address) of a tunnel hop (everything after '@')
   */
  private static final Pattern DOMAIN_PATTERN = java.util.regex.Pattern.compile(DOMAIN + "|" + IP_DOMAIN,
      CASE_INSENSITIVE);

  @Override
  public void initialize(TunnelRoute annotation) {}

  @Override
  public boolean isValid(CharSequence value, ConstraintValidatorContext context) {
    if ((value == null) || (value.toString().trim().length() == 0)) {
      return true;
    }

    String[] hops = value.toString().split("->");
    for (String userHostInfo : hops) {
      userHostInfo = userHostInfo.trim();

      if (userHostInfo.isEmpty()) {
        return false;
      }

      String username = null;
      String hostname = null;
      String[] parts = userHostInfo.split("@");
      switch (parts.length) {
      case 1:
        hostname = parts[0];
        break;
      case 2:
        username = parts[0];
        hostname = parts[1];
        break;
      default:
        // invalid format
        return false;
      }

      // If we have a trailing dot in local or domain part we have an invalid tunnel hop.
      // The regular expression match would take care of this, but IDN.toASCII drops the trailing '.'
      // (IMO a bug in the implementation)

      if ((username != null)
          && (username.endsWith(".") || !matchPart(username.trim(), LOCAL_PATTERN, MAX_LOCAL_PART_LENGTH))) {
        return false;
      }

      if (hostname.endsWith(".") || !matchPart(hostname.trim(), DOMAIN_PATTERN, MAX_DOMAIN_PART_LENGTH)) {
        return false;
      }
    }

    return true;
  }

  private boolean matchPart(String part, Pattern pattern, int maxLength) {
    String asciiString;
    try {
      asciiString = toAscii(part);
    } catch (IllegalArgumentException e) {
      return false;
    }

    if (asciiString.length() > maxLength) {
      return false;
    }

    Matcher matcher = pattern.matcher(asciiString);
    return matcher.matches();
  }

  private String toAscii(String unicodeString) throws IllegalArgumentException {
    StringBuilder asciiString = new StringBuilder(256);
    int start = 0;
    int end = unicodeString.length() <= 63 ? unicodeString.length() : 63;
    while (true) {
      // IDN.toASCII only supports a max "label" length of 63 characters. Need to chunk the input in these sizes
      asciiString.append(IDN.toASCII(unicodeString.substring(start, end)));
      if (end == unicodeString.length()) {
        break;
      }
      start = end;
      end = (start + 63) > unicodeString.length() ? unicodeString.length() : start + 63;
    }

    return asciiString.toString();
  }
}
