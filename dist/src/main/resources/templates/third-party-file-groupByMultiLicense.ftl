<#-- Format artifact "name (groupId:artifactId:version - url)" -->
<#function artifactFormat artifact>
    <#if artifact.name?index_of('Unnamed') &gt; -1>
        <#return artifact.artifactId + " (" + artifact.groupId + ":" + artifact.artifactId + ":" + artifact.version + " - " + (artifact.url!"no url defined") + ")">
    <#else>
        <#return artifact.name + " (" + artifact.groupId + ":" + artifact.artifactId + ":" + artifact.version + " - " + (artifact.url!"no url defined") + ")">
    </#if>
</#function>

<#-- Create a key from provided licenses list, ordered alphabetically: "license A, license B, license C" -->
<#function licensesKey licenses>
    <#local result = "">
    <#list licenses?sort as license>
        <#local result=result + ", " + license>
    </#list>
    <#return result?substring(2)>
</#function>

<#-- Aggregate dependencies map for generated license key (support for multi-license) and convert artifact to string -->
<#function aggregateLicenses dependencies>
    <#assign aggregate = {}>
    <#list dependencyMap as entry>
        <#assign project = artifactFormat(entry.getKey())/>
        <#assign licenses = entry.getValue()/>
        <#assign key = licensesKey(licenses)/>
        <#if aggregate[key]?? >
            <#assign replacement = aggregate[key] + [project] />
            <#assign aggregate = aggregate + {key:replacement} />
        <#else>
            <#assign aggregate = aggregate + {key:[project]} />
        </#if>
    </#list>
    <#return aggregate>
</#function>

List of third-party dependencies grouped by their license type.

<#if dependencyMap?size == 0>
  The project has no dependencies.
<#else>
    <#assign aggregate = aggregateLicenses(dependencyMap)>
<#-- Print sorted aggregate licenses -->
    <#list aggregate?keys?sort as licenses>
        <#assign projects = aggregate[licenses]/>

        ${licenses}

    <#-- Print sorted projects -->
        <#list projects?sort as project>
        * ${project}
        </#list>
    </#list>
</#if>