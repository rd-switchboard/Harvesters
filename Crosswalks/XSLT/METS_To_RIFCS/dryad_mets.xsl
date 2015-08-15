<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"	
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform" 
    xmlns:oai="http://www.openarchives.org/OAI/2.0/"  
    xmlns:mets="http://www.loc.gov/METS/"
    xmlns:mods="http://www.loc.gov/mods/v3" 
    xmlns:xlink="http://www.w3.org/1999/xlink" 
    xmlns="http://ands.org.au/standards/rif-cs/registryObjects"
    exclude-result-prefixes="xs xsi xsl oai mets mods xlink">

    <!-- =========================================== -->
    <!-- Configuration                               -->
    <!-- =========================================== -->

    <xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes"/>
    <xsl:param name="global_group" select="'Dryad'"/>
    <!-- <xsl:param name="global_acronym" select="'Dryad'"/> -->
    <xsl:param name="originatingSource" select="'Dryad'"/> 

    <!-- =========================================== -->
    <!-- Copy Everything		             -->
    <!-- =========================================== -->

    <xsl:template match="@*|node()">
        <xsl:copy>
            <xsl:apply-templates select="@*|node()"/>
        </xsl:copy>
    </xsl:template>

    <!-- =========================================== -->
    <!-- RegistryObjects (root) Template             -->
    <!-- =========================================== -->
    
    <xsl:template match="oai:record">
	<xsl:variable name="recordKey" select="./oai:header/oai:identifier/text()"/>

	<record>
	    <xsl:copy-of select="./oai:header"/>
	    <metadata>	
                <registryObjects>
                    <xsl:attribute name="xsi:schemaLocation">
                        <xsl:text>http://ands.org.au/standards/rif-cs/registryObjects http://services.ands.org.au/documentation/rifcs/schema/registryObjects.xsd</xsl:text>
                    </xsl:attribute>
		
                    <!-- <xsl:for-each select=""> -->
                    <!-- <xsl:if test=".//mods:genre='Dataset'"> -->
                        <registryObject>
	   
			    <!-- registryObject:@group -->
			    <xsl:attribute name="group">
				<xsl:value-of select="$global_group"/>
			    </xsl:attribute>

		    	    <!-- <xsl:apply-templates select=".//mods:identifier[not(@type)]" mode="registryObject_key"/> -->
		    	    <key>
		        	<xsl:copy-of select="$recordKey"/>
		    	    </key>

		    	    <originatingSource>
		       		<xsl:value-of select="$originatingSource"/>    
		    	    </originatingSource>
	
		    	    <collection>
				<xsl:attribute name="type">
			   	    <xsl:text>collection</xsl:text>
		        	 </xsl:attribute>

			        <xsl:apply-templates select=".//mods:identifier" mode="identifier"/> 	
			        <xsl:apply-templates select=".//mods:identifier[@type='uri']" mode="url"/>
                	        <xsl:apply-templates select=".//mets:mets" mode="dataset_description"/> 
			        <xsl:apply-templates select=".//mets:mets" mode="dates.submitted"/>
				<xsl:apply-templates select=".//mets:mets" mode="dates.available"/>
				<xsl:apply-templates select=".//mets:mets" mode="dates.issued"/>
				<xsl:apply-templates select=".//mets:mets" mode="name"/> 
				<xsl:apply-templates select=".//mods:subject/mods:topic"/>
				<xsl:if test=".//mods:geographic or .//mods:temporal">
		  		    <xsl:apply-templates select=".//mets:mets" mode="coverage"/>
				</xsl:if>
				<xsl:apply-templates select=".//mods:relatedItem"/>
				<xsl:apply-templates select=".//mods:accessCondition"/>
			    </collection>
			</registryObject>
                    <!-- </xsl:if> -->
                    <!-- </xsl:for-each> -->
                </registryObjects>
            </metadata>
        </record>
    </xsl:template>
 
    <!-- =========================================== -->
    <!-- RegistryObject/key template                 -->
    <!-- =========================================== -->

    <!-- <xsl:template match="mets:dmdSec" mode="registryObject_key">
        <key>
	    <xsl:variable name="identificator" select="@ID"/> 	
            <xsl:value-of select="concat($global_acronym,'/', $identificator)"/>
        </key>
    </xsl:template> -->

    <!-- <xsl:template match="mods:identifier" mode="registryObject_key">
        <key>
            <xsl:value-of select="normalize-space(.)"/>
        </key>
    </xsl:template> -->

    <!-- =========================================== -->
    <!-- RegistryObject/identifier(doi) template     -->
    <!-- =========================================== -->

    <xsl:template match="mods:identifier" mode="identifier">
        <identifier>
	    <xsl:variable name="identifier" select="normalize-space(.)"/>	
	    <xsl:attribute name="type">
		<xsl:choose>
		    <xsl:when test="contains($identifier, 'doi')">
		        <xsl:text>doi</xsl:text>
                    </xsl:when>   
		    <xsl:when test="contains($identifier, 'hdl.handle.net')">
		        <xsl:text>handle</xsl:text>
                    </xsl:when>  
		    <xsl:otherwise>
			<xsl:text>local</xsl:text>
		    </xsl:otherwise> 
		</xsl:choose>
            </xsl:attribute>
            <xsl:value-of select="normalize-space(.)"/>
        </identifier>
    </xsl:template>

    <!-- =========================================== -->
    <!-- RegistryObject/location/address (url)       -->
    <!-- =========================================== -->

    <xsl:template match="mods:identifier" mode="url">	
	<location>
	    <address>
		<electronic>
		     <xsl:attribute name="type">
			<xsl:text>url</xsl:text>
            	     </xsl:attribute>
		     <value>	
		     	<xsl:value-of select="normalize-space(.)"/>
		     </value>
		</electronic>
	    </address>
	</location>
    </xsl:template>

    <!-- =========================================== -->
    <!-- RegistryObject/description template         -->
    <!-- =========================================== -->

    <!-- <xsl:template match="mods:abstract">
	<description>
	    <xsl:attribute name="type">
		<xsl:text>full</xsl:text>
            </xsl:attribute>
	    <xsl:value-of select="."/>
	</description>
    </xsl:template> -->

    <!-- =========================================== -->
    <!-- RegistryObject/description template         -->
    <!-- =========================================== -->

    <xsl:template match="mets:mets" mode="dataset_description">
	<xsl:variable name="newLine" select=""/>
	
	<xsl:if test=".//mods:note or .//mods:abstract or .//mods:role/mods:roleTerm='author'">
	    <description>
	        <xsl:attribute name="type">
		    <xsl:text>full</xsl:text>
                </xsl:attribute>
		<xsl:if test=".//mods:note">
		    <xsl:value-of select=".//mods:note/text()"/>
		    <xsl:variable name="newLine" select="'&#xa;'"/> 	
		</xsl:if>
		<xsl:if test=".//mods:abstrac">
		    <xsl:value-of select="$newLine"/>	
		    <xsl:value-of select=".//mods:abstract/text()"/>
		    <xsl:variable name="newLine" select="'&#xa;'"/> 
		</xsl:if>
		<xsl:if test=".//mods:role/mods:roleTerm='author'">
		    <xsl:value-of select="$newLine"/>
   	            <xsl:for-each select=".//mods:name">
			<xsl:if test="mods:role/mods:roleTerm='author'">
		            <xsl:text>&#xa;Author: </xsl:text>
			    <xsl:value-of select="mods:namePart"/>	
 		        </xsl:if>
	            </xsl:for-each>
		</xsl:if>
	     </description>
	</xsl:if>
    </xsl:template> 	

    <!-- =========================================== -->
    <!-- RegistryObject/dates (dc.dateSubmitted) template    -->
    <!-- =========================================== -->

    <xsl:template match="mets:mets" mode="dates.submitted">
	<dates>
	     <xsl:attribute name="type">
		<xsl:text>dc.dateSubmitted</xsl:text>
            </xsl:attribute>
	    <xsl:apply-templates select="mets:metsHdr"/>
	</dates>
    </xsl:template> 	

    <!-- =========================================== -->
    <!-- RegistryObject/dates (dc.available) template    -->
    <!-- =========================================== -->

    <xsl:template match="mets:mets" mode="dates.available">
	<dates>
	     <xsl:attribute name="type">
		<xsl:text>dc.available</xsl:text>
            </xsl:attribute>
	    <xsl:apply-templates select=".//mods:dateAvailable"/>
	</dates>
    </xsl:template> 	

    <!-- =========================================== -->
    <!-- RegistryObject/dates (dc.issued) template    -->
    <!-- =========================================== -->

    <xsl:template match="mets:mets" mode="dates.issued">
	<dates>
	     <xsl:attribute name="type">
		<xsl:text>dc.issued</xsl:text>
            </xsl:attribute>
	    <xsl:apply-templates select=".//mods:dateIssued"/>
	</dates>
    </xsl:template> 	

    <!-- =========================================== -->
    <!-- RegistryObject/dates/date(dc.dateSubmitted) -->
    <!-- =========================================== -->

    <xsl:template match="mets:metsHdr">
        <date>
	    <xsl:attribute name="type">
		<xsl:text>dateFrom</xsl:text>
            </xsl:attribute>
	    <xsl:attribute name="dateFormat">
		<xsl:text>W3CDTF</xsl:text>
            </xsl:attribute>
	    <xsl:value-of select="normalize-space(@CREATEDATE)"/>	
	</date>
    </xsl:template>

    <!-- =========================================== -->
    <!-- RegistryObject/dates/date(dc.available)     -->
    <!-- =========================================== -->

    <xsl:template match="mods:dateAvailable">
        <date>
	    <xsl:attribute name="type">
		<xsl:text>dateFrom</xsl:text>
            </xsl:attribute>
	    <xsl:attribute name="dateFormat">
		<xsl:text>W3CDTF</xsl:text>
            </xsl:attribute>
	    <xsl:value-of select="normalize-space(.)"/>	
	</date>
    </xsl:template>

    <!-- =========================================== -->
    <!-- RegistryObject/dates/date(dc.issued)        -->
    <!-- =========================================== -->

    <xsl:template match="mods:dateIssued">
        <date>
	    <xsl:attribute name="type">
		<xsl:text>dateFrom</xsl:text>
            </xsl:attribute>
	    <xsl:attribute name="dateFormat">
		<xsl:text>W3CDTF</xsl:text>
            </xsl:attribute>
	    <xsl:value-of select="normalize-space(.)"/>	
	</date>
    </xsl:template>

    <!-- =========================================== -->
    <!-- RegistryObject/name template               -->
    <!-- =========================================== -->

    <xsl:template match="mets:mets" mode="name">
	<xsl:if test=".//mods:titleInfo">
	    <name>
	        <xsl:apply-templates select=".//mods:titleInfo"/>
    	    </name>
	</xsl:if>
    </xsl:template>

    <!-- =========================================== -->
    <!-- RegistryObject/name/namePart(primary) -->
    <!-- =========================================== -->

    <xsl:template match="mods:titleInfo">
	<namePart>
	    <xsl:attribute name="type">
		<xsl:text>primary</xsl:text>
            </xsl:attribute>
	    <xsl:value-of select="normalize-space(.)"/>
	</namePart>	
    </xsl:template>


    <!-- =========================================== -->
    <!-- RegistryObject/subject                     -->
    <!-- =========================================== -->

    <xsl:template match="mods:topic">
	<subject>
	    <xsl:attribute name="type">
		<xsl:text>local</xsl:text>
            </xsl:attribute>	
	    <xsl:value-of select="normalize-space(.)"/>
	</subject>	
    </xsl:template>	


    <!-- =========================================== -->
    <!-- RegistryObject/coverage template            -->
    <!-- =========================================== -->

    <xsl:template match="mets:mets" mode="coverage">
	<coverage>
	    <xsl:apply-templates select=".//mods:geographic"/>
	    <xsl:apply-templates select=".//mods:temporal"/>
	</coverage>
    </xsl:template>

   
    <!-- =========================================== -->
    <!-- RegistryObject/coverage/spatial             -->
    <!-- =========================================== -->

    <xsl:template match="mods:geographic">
	<spatial>
	    <xsl:attribute name="type">
		<xsl:text>text</xsl:text>
            </xsl:attribute>
	    <xsl:value-of select="normalize-space(.)"/>
	</spatial>	
    </xsl:template>

    <!-- =========================================== -->
    <!-- RegistryObject/coverage/temporal/text       -->
    <!-- =========================================== -->

    <xsl:template match="mods:temporal">
	<temporal>
	    <text>
		<xsl:value-of select="normalize-space(.)"/>
	    </text>
	</temporal>	
    </xsl:template>

    <!-- =========================================== -->
    <!-- RegistryObject/relatedObject                -->
    <!-- =========================================== -->

    <xsl:template match="mods:relatedItem">
	<relatedObject>
	    <key>
		<xsl:value-of select="normalize-space(.)"/>
	    </key>
	    <!-- <xsl:variable name="type" select="@type"/> -->
	    <relation>
		<xsl:attribute name="type">
		    <xsl:choose>
		        <xsl:when test="@type='host'">
		            <xsl:text>isPartOf</xsl:text>
                        </xsl:when>   
		        <xsl:when test="@type='constituent'">
		            <xsl:text>hasPart</xsl:text>
                        </xsl:when>   
		        <!-- <xsl:when test="@type!=''">
			    <xsl:value-of select="@type"/>		        
                        </xsl:when> -->
		        <xsl:otherwise>
			    <xsl:text>hasAssociationWith</xsl:text>
		        </xsl:otherwise> 
		    </xsl:choose>
		</xsl:attribute>    
	    </relation>
	</relatedObject>	
    </xsl:template>	

    <!-- =========================================== -->
    <!-- RegistryObject/rights                       -->
    <!-- =========================================== -->

    <xsl:template match="mods:accessCondition">
	<rights>
	    <rightsStatement>
	        <xsl:value-of select="normalize-space(.)"/>
	    </rightsStatement>
	</rights>	
    </xsl:template>
</xsl:stylesheet>
